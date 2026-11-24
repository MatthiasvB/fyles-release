use std::marker::PhantomData;

use asynchronous_codec::{Decoder, Encoder};
use bytes::{Buf, BufMut, BytesMut};
use crypto::stream::{CounterExhausted, DirectionKeys, OpenError, Opener, Sealer, TAG_LEN};

use crate::constants::{MAX_INCOMING_FRAME_PAYLOAD, STREAM_FRAME_PAYLOAD};

const LEN_PREFIX: usize = 4;
const MIN_FRAME_LEN: usize = 1 + TAG_LEN;
const MAX_OUTGOING_FRAME_LEN: usize = 1 + STREAM_FRAME_PAYLOAD + TAG_LEN;
const MAX_INCOMING_FRAME_LEN: usize = 1 + MAX_INCOMING_FRAME_PAYLOAD + TAG_LEN;

#[derive(Clone, Copy)]
#[repr(u8)]
enum DataKind {
    Begin = 0,
    Data = 1,
    End = 2,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum ReportKind {
    Progress = 0,
    Committed = 1,
    Failed = 2,
}

pub enum Outgoing<'a> {
    Begin,
    Data(&'a [u8]),
    End,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    Begin,
    Data(BytesMut),
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Report {
    Progress(u64),
    Committed,
    Failed(Failure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Failure {
    Violation = 0,
    Internal = 1,
}

impl From<DataKind> for u8 {
    fn from(kind: DataKind) -> u8 {
        kind as u8
    }
}

impl From<ReportKind> for u8 {
    fn from(kind: ReportKind) -> u8 {
        kind as u8
    }
}

impl From<Failure> for u8 {
    fn from(failure: Failure) -> u8 {
        failure as u8
    }
}

impl TryFrom<u8> for DataKind {
    type Error = FrameError;

    fn try_from(byte: u8) -> Result<Self, FrameError> {
        match byte {
            0 => Ok(Self::Begin),
            1 => Ok(Self::Data),
            2 => Ok(Self::End),
            _ => Err(Violation::Malformed.into()),
        }
    }
}

impl TryFrom<u8> for ReportKind {
    type Error = FrameError;

    fn try_from(byte: u8) -> Result<Self, FrameError> {
        match byte {
            0 => Ok(Self::Progress),
            1 => Ok(Self::Committed),
            2 => Ok(Self::Failed),
            _ => Err(Violation::Malformed.into()),
        }
    }
}

impl TryFrom<u8> for Failure {
    type Error = FrameError;

    fn try_from(byte: u8) -> Result<Self, FrameError> {
        match byte {
            0 => Ok(Self::Violation),
            1 => Ok(Self::Internal),
            _ => Err(Violation::Malformed.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("stream I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Unproven(#[from] Unproven),
    #[error(transparent)]
    Violation(#[from] Violation),
}

/// Raised before a frame authenticates, so anyone on the path can cause it.
#[derive(Debug, thiserror::Error)]
pub enum Unproven {
    #[error("frame length {0} is out of bounds")]
    BadLength(usize),
    #[error("frame does not authenticate")]
    Unauthentic,
}

/// Only the holder of the attempt's key can cause it.
#[derive(Debug, thiserror::Error)]
pub enum Violation {
    #[error("frame content does not fit its kind")]
    Malformed,
    #[error("frame counter exhausted")]
    CounterExhausted,
}

impl From<CounterExhausted> for FrameError {
    fn from(_: CounterExhausted) -> Self {
        Violation::CounterExhausted.into()
    }
}

impl From<OpenError> for FrameError {
    fn from(error: OpenError) -> Self {
        match error {
            OpenError::CounterExhausted(exhausted) => exhausted.into(),
            OpenError::Unauthentic => Unproven::Unauthentic.into(),
        }
    }
}

struct FrameSealer<K> {
    sealer: Sealer,
    kind: PhantomData<K>,
}

struct FrameOpener<K> {
    opener: Opener,
    kind: PhantomData<K>,
}

impl<K: Into<u8>> FrameSealer<K> {
    fn seal(&mut self, kind: K, payload: &[u8], dst: &mut BytesMut) -> Result<(), FrameError> {
        let len = 1 + payload.len() + TAG_LEN;
        if len > MAX_OUTGOING_FRAME_LEN {
            return Err(Unproven::BadLength(len).into());
        }
        let start = dst.len();
        dst.reserve(LEN_PREFIX + len);
        dst.put_u32(len as u32);
        let plaintext = dst.len();
        dst.put_u8(kind.into());
        dst.put_slice(payload);
        match self.sealer.seal_in_place(&mut dst[plaintext..]) {
            Ok(tag) => {
                dst.put_slice(&tag);
                Ok(())
            }
            Err(exhausted) => {
                dst.truncate(start);
                Err(exhausted.into())
            }
        }
    }
}

impl<K: TryFrom<u8, Error = FrameError>> FrameOpener<K> {
    fn open(&mut self, src: &mut BytesMut) -> Result<Option<(K, BytesMut)>, FrameError> {
        let Some(prefix) = src.first_chunk::<LEN_PREFIX>() else {
            return Ok(None);
        };
        let len = u32::from_be_bytes(*prefix) as usize;
        if !(MIN_FRAME_LEN..=MAX_INCOMING_FRAME_LEN).contains(&len) {
            return Err(Unproven::BadLength(len).into());
        }
        if src.len() < LEN_PREFIX + len {
            src.reserve(LEN_PREFIX + len - src.len());
            return Ok(None);
        }
        src.advance(LEN_PREFIX);
        let mut frame = src.split_to(len);
        let plaintext = self.opener.open_in_place(&mut frame)?;
        frame.truncate(plaintext);
        let kind = K::try_from(frame[0])?;
        frame.advance(1);
        Ok(Some((kind, frame)))
    }
}

pub struct DataEncoder(FrameSealer<DataKind>);
pub struct DataDecoder(FrameOpener<DataKind>);
pub struct ReportEncoder(FrameSealer<ReportKind>);
pub struct ReportDecoder(FrameOpener<ReportKind>);

fn halves<Out, In>(keys: DirectionKeys) -> (FrameSealer<Out>, FrameOpener<In>) {
    let (sealer, opener) = keys.into_halves();
    (
        FrameSealer {
            sealer,
            kind: PhantomData,
        },
        FrameOpener {
            opener,
            kind: PhantomData,
        },
    )
}

pub fn sender(keys: DirectionKeys) -> (DataEncoder, ReportDecoder) {
    let (sealer, opener) = halves(keys);
    (DataEncoder(sealer), ReportDecoder(opener))
}

pub fn receiver(keys: DirectionKeys) -> (ReportEncoder, DataDecoder) {
    let (sealer, opener) = halves(keys);
    (ReportEncoder(sealer), DataDecoder(opener))
}

impl Encoder for DataEncoder {
    type Item<'a> = Outgoing<'a>;
    type Error = FrameError;

    fn encode(&mut self, item: Outgoing<'_>, dst: &mut BytesMut) -> Result<(), FrameError> {
        match item {
            Outgoing::Begin => self.0.seal(DataKind::Begin, &[], dst),
            Outgoing::Data([]) => Err(Violation::Malformed.into()),
            Outgoing::Data(data) => self.0.seal(DataKind::Data, data, dst),
            Outgoing::End => self.0.seal(DataKind::End, &[], dst),
        }
    }
}

impl Decoder for DataDecoder {
    type Item = Incoming;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Incoming>, FrameError> {
        let Some((kind, payload)) = self.0.open(src)? else {
            return Ok(None);
        };
        let incoming = match (kind, payload.is_empty()) {
            (DataKind::Begin, true) => Incoming::Begin,
            (DataKind::Data, false) => Incoming::Data(payload),
            (DataKind::End, true) => Incoming::End,
            _ => return Err(Violation::Malformed.into()),
        };
        Ok(Some(incoming))
    }
}

impl Encoder for ReportEncoder {
    type Item<'a> = Report;
    type Error = FrameError;

    fn encode(&mut self, item: Report, dst: &mut BytesMut) -> Result<(), FrameError> {
        match item {
            Report::Progress(bytes) => self.0.seal(ReportKind::Progress, &bytes.to_be_bytes(), dst),
            Report::Committed => self.0.seal(ReportKind::Committed, &[], dst),
            Report::Failed(failure) => self.0.seal(ReportKind::Failed, &[failure.into()], dst),
        }
    }
}

impl Decoder for ReportDecoder {
    type Item = Report;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Report>, FrameError> {
        let Some((kind, payload)) = self.0.open(src)? else {
            return Ok(None);
        };
        let report = match (kind, &payload[..]) {
            (ReportKind::Progress, bytes) => Report::Progress(u64::from_be_bytes(
                bytes.try_into().map_err(|_| Violation::Malformed)?,
            )),
            (ReportKind::Committed, []) => Report::Committed,
            (ReportKind::Failed, [code]) => Report::Failed(Failure::try_from(*code)?),
            _ => return Err(Violation::Malformed.into()),
        };
        Ok(Some(report))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AttemptGrant;
    use crypto::stream::Role;

    struct Ends {
        data_out: DataEncoder,
        report_in: ReportDecoder,
        report_out: ReportEncoder,
        data_in: DataDecoder,
    }

    fn ends() -> Ends {
        let grant = AttemptGrant::mint();
        let (data_out, report_in) = sender(grant.keys(Role::Sender));
        let (report_out, data_in) = receiver(grant.keys(Role::Receiver));
        Ends {
            data_out,
            report_in,
            report_out,
            data_in,
        }
    }

    fn sealed(encoder: &mut DataEncoder, item: Outgoing<'_>) -> BytesMut {
        let mut buf = BytesMut::new();
        encoder.encode(item, &mut buf).unwrap();
        buf
    }

    #[test]
    fn every_frame_kind_round_trips() {
        let mut ends = ends();
        let mut wire = sealed(&mut ends.data_out, Outgoing::Begin);
        wire.extend_from_slice(&sealed(&mut ends.data_out, Outgoing::Data(b"payload")));
        wire.extend_from_slice(&sealed(&mut ends.data_out, Outgoing::End));
        assert_eq!(
            ends.data_in.decode(&mut wire).unwrap(),
            Some(Incoming::Begin)
        );
        assert_eq!(
            ends.data_in.decode(&mut wire).unwrap(),
            Some(Incoming::Data(BytesMut::from(&b"payload"[..])))
        );
        assert_eq!(ends.data_in.decode(&mut wire).unwrap(), Some(Incoming::End));
        assert_eq!(ends.data_in.decode(&mut wire).unwrap(), None);

        let mut back = BytesMut::new();
        for report in [
            Report::Progress(42),
            Report::Committed,
            Report::Failed(Failure::Violation),
            Report::Failed(Failure::Internal),
        ] {
            ends.report_out.encode(report, &mut back).unwrap();
            assert_eq!(ends.report_in.decode(&mut back).unwrap(), Some(report));
        }
    }

    #[test]
    fn a_partial_frame_waits_for_the_rest() {
        let mut ends = ends();
        let whole = sealed(&mut ends.data_out, Outgoing::Data(b"payload"));
        let mut wire = BytesMut::from(&whole[..whole.len() - 1]);
        assert_eq!(ends.data_in.decode(&mut wire).unwrap(), None);
        wire.extend_from_slice(&whole[whole.len() - 1..]);
        assert!(matches!(
            ends.data_in.decode(&mut wire).unwrap(),
            Some(Incoming::Data(_))
        ));
    }

    #[test]
    fn a_flipped_bit_does_not_authenticate() {
        let mut ends = ends();
        let mut wire = sealed(&mut ends.data_out, Outgoing::Data(b"payload"));
        wire[LEN_PREFIX + 2] ^= 1;
        assert!(matches!(
            ends.data_in.decode(&mut wire),
            Err(FrameError::Unproven(Unproven::Unauthentic))
        ));
    }

    #[test]
    fn reordered_frames_do_not_authenticate() {
        let mut ends = ends();
        sealed(&mut ends.data_out, Outgoing::Begin);
        let mut second = sealed(&mut ends.data_out, Outgoing::Data(b"payload"));
        assert!(matches!(
            ends.data_in.decode(&mut second),
            Err(FrameError::Unproven(Unproven::Unauthentic))
        ));
    }

    #[test]
    fn a_replayed_frame_does_not_authenticate() {
        let mut ends = ends();
        let first = sealed(&mut ends.data_out, Outgoing::Begin);
        ends.data_in.decode(&mut first.clone()).unwrap();
        assert!(matches!(
            ends.data_in.decode(&mut first.clone()),
            Err(FrameError::Unproven(Unproven::Unauthentic))
        ));
    }

    #[test]
    fn keys_of_another_grant_do_not_authenticate() {
        let mut ends = ends();
        let mut stranger = self::ends();
        let mut wire = sealed(&mut ends.data_out, Outgoing::Begin);
        assert!(matches!(
            stranger.data_in.decode(&mut wire),
            Err(FrameError::Unproven(Unproven::Unauthentic))
        ));
    }

    #[test]
    fn a_frame_cannot_be_read_back_with_its_own_direction_key() {
        let grant = AttemptGrant::mint();
        let (mut data_out, _) = sender(grant.keys(Role::Sender));
        let (_, mut mirrored) = receiver(grant.keys(Role::Sender));
        let mut wire = sealed(&mut data_out, Outgoing::Begin);
        assert!(matches!(
            mirrored.decode(&mut wire),
            Err(FrameError::Unproven(Unproven::Unauthentic))
        ));
    }

    #[test]
    fn an_out_of_bounds_length_is_refused_before_anything_is_buffered() {
        let mut ends = ends();
        let mut wire = BytesMut::new();
        wire.put_u32((MAX_INCOMING_FRAME_LEN + 1) as u32);
        let capacity = wire.capacity();
        assert!(matches!(
            ends.data_in.decode(&mut wire),
            Err(FrameError::Unproven(Unproven::BadLength(_)))
        ));
        assert_eq!(wire.capacity(), capacity);
    }

    #[test]
    fn a_frame_larger_than_we_send_is_accepted() {
        let grant = AttemptGrant::mint();
        let (mut sealer, _) = grant.keys(Role::Sender).into_halves();
        let (_, mut data_in) = receiver(grant.keys(Role::Receiver));
        let payload = vec![7; STREAM_FRAME_PAYLOAD + 1];
        let mut plaintext = [&[DataKind::Data.into()], &payload[..]].concat();
        let tag = sealer.seal_in_place(&mut plaintext).unwrap();
        let mut wire = BytesMut::new();
        wire.put_u32((plaintext.len() + TAG_LEN) as u32);
        wire.put_slice(&plaintext);
        wire.put_slice(&tag);
        assert_eq!(
            data_in.decode(&mut wire).unwrap(),
            Some(Incoming::Data(BytesMut::from(&payload[..])))
        );
    }

    #[test]
    fn content_that_does_not_fit_its_kind_is_malformed() {
        let mut ends = ends();
        assert!(matches!(
            ends.data_out
                .encode(Outgoing::Data(&[]), &mut BytesMut::new()),
            Err(FrameError::Violation(Violation::Malformed))
        ));

        let mut wire = BytesMut::new();
        ends.report_out
            .0
            .seal(ReportKind::Progress, &[1, 2, 3], &mut wire)
            .unwrap();
        assert!(matches!(
            ends.report_in.decode(&mut wire),
            Err(FrameError::Violation(Violation::Malformed))
        ));
    }
}
