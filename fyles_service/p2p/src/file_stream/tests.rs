use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use asynchronous_codec::{Encoder, FramedRead, FramedWrite};
use bytes::BytesMut;
use futures::{AsyncReadExt, AsyncWriteExt, SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::fs::{File, OpenOptions};
use tokio::io::DuplexStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use tokio_util::sync::CancellationToken;

use crate::CHUNK_SIZE;
use crate::constants::STREAM_FRAME_PAYLOAD;
use crate::file_stream::codec::{self, Failure, Incoming, Outgoing, Report};
use crate::file_stream::receive::{
    BeginError, ReceiveExit, ReceiverEnds, ReceivingFile, Unfinished, await_begin, receive,
};
use crate::file_stream::send::{AttemptOutcome, run_send_attempt};
use crate::source::Source;
use crate::types::AttemptGrant;
use crypto::stream::Role;

type Pipe = Compat<DuplexStream>;

fn pipe() -> (Pipe, Pipe) {
    let (a, b) = tokio::io::duplex(256 * 1024);
    (a.compat(), b.compat())
}

fn content(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn source_file(dir: &TempDir, bytes: &[u8]) -> (PathBuf, Source) {
    let path = dir.path().join("source");
    std::fs::write(&path, bytes).unwrap();
    let source = Source::open(File::open(&path).await.unwrap())
        .await
        .unwrap();
    (path, source)
}

async fn part_file(dir: &TempDir, prefix: &[u8], declared: usize) -> (PathBuf, ReceivingFile) {
    let path = dir.path().join("part");
    std::fs::write(&path, prefix).unwrap();
    let file = OpenOptions::new().append(true).open(&path).await.unwrap();
    let receiving = ReceivingFile::resuming(file, prefix.len() as u64, declared as u64).unwrap();
    (path, receiving)
}

async fn admitted(
    mut stream: Pipe,
    grant: &AttemptGrant,
) -> Result<ReceiverEnds<Pipe>, BeginError> {
    let mut id = [0u8; 16];
    stream.read_exact(&mut id).await.unwrap();
    assert_eq!(&id, grant.attempt.as_bytes());
    let mut ends = ReceiverEnds::new(stream, grant);
    await_begin(&mut ends).await?;
    Ok(ends)
}

async fn receive_all(stream: Pipe, grant: &AttemptGrant, file: &mut ReceivingFile) -> ReceiveExit {
    let mut ends = admitted(stream, grant).await.expect("a valid begin");
    receive(
        &mut ends,
        file,
        |_| {},
        async || true,
        &CancellationToken::new(),
    )
    .await
}

async fn send(stream: Pipe, grant: &AttemptGrant, source: Source, offset: u64) -> AttemptOutcome {
    run_send_attempt(
        stream,
        grant,
        source,
        offset,
        &AtomicU64::new(offset),
        |_| {},
        &CancellationToken::new(),
    )
    .await
    .expect("an attempt nobody cancels to settle")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

#[tokio::test]
async fn a_whole_file_arrives_intact() {
    let dir = TempDir::new().unwrap();
    let bytes = content(3 * CHUNK_SIZE + 123, 1);
    let (_, source) = source_file(&dir, &bytes).await;
    let (part, mut receiving) = part_file(&dir, &[], bytes.len()).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (outcome, exit) = tokio::join!(
        send(a, &grant, source, 0),
        receive_all(b, &grant, &mut receiving)
    );

    assert!(matches!(outcome, AttemptOutcome::Committed), "{outcome:?}");
    assert_eq!(exit, ReceiveExit::Committed);
    assert!(read(&part) == bytes);
}

#[tokio::test]
async fn a_resumed_file_arrives_intact() {
    let dir = TempDir::new().unwrap();
    let bytes = content(2 * CHUNK_SIZE + 7, 2);
    let offset = CHUNK_SIZE + 3;
    let (_, source) = source_file(&dir, &bytes).await;
    let (part, mut receiving) = part_file(&dir, &bytes[..offset], bytes.len()).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (outcome, exit) = tokio::join!(
        send(a, &grant, source, offset as u64),
        receive_all(b, &grant, &mut receiving)
    );

    assert!(matches!(outcome, AttemptOutcome::Committed), "{outcome:?}");
    assert_eq!(exit, ReceiveExit::Committed);
    assert!(read(&part) == bytes);
}

#[tokio::test]
async fn an_empty_file_commits() {
    let dir = TempDir::new().unwrap();
    let (_, source) = source_file(&dir, &[]).await;
    let (_, mut receiving) = part_file(&dir, &[], 0).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (outcome, exit) = tokio::join!(
        send(a, &grant, source, 0),
        receive_all(b, &grant, &mut receiving)
    );

    assert!(matches!(outcome, AttemptOutcome::Committed), "{outcome:?}");
    assert_eq!(exit, ReceiveExit::Committed);
}

#[tokio::test]
async fn a_resume_at_the_end_commits_without_data() {
    let dir = TempDir::new().unwrap();
    let bytes = content(1000, 3);
    let (_, source) = source_file(&dir, &bytes).await;
    let (part, mut receiving) = part_file(&dir, &bytes, bytes.len()).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (outcome, exit) = tokio::join!(
        send(a, &grant, source, bytes.len() as u64),
        receive_all(b, &grant, &mut receiving)
    );

    assert!(matches!(outcome, AttemptOutcome::Committed), "{outcome:?}");
    assert_eq!(exit, ReceiveExit::Committed);
    assert!(read(&part) == bytes);
}

#[tokio::test]
async fn a_source_that_grew_after_it_was_opened_is_not_committed() {
    let dir = TempDir::new().unwrap();
    let bytes = content(CHUNK_SIZE + 10, 4);
    let (path, source) = source_file(&dir, &bytes).await;
    std::fs::write(&path, content(CHUNK_SIZE + 20, 5)).unwrap();
    let (_, mut receiving) = part_file(&dir, &[], bytes.len()).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (outcome, exit) = tokio::join!(
        send(a, &grant, source, 0),
        receive_all(b, &grant, &mut receiving)
    );

    assert!(
        matches!(outcome, AttemptOutcome::SourceChanged),
        "{outcome:?}"
    );
    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Interrupted));
}

#[tokio::test]
async fn a_source_that_shrank_is_not_committed() {
    let dir = TempDir::new().unwrap();
    let bytes = content(2 * CHUNK_SIZE, 6);
    let (path, source) = source_file(&dir, &bytes).await;
    std::fs::write(&path, &bytes[..CHUNK_SIZE / 2]).unwrap();
    let (_, mut receiving) = part_file(&dir, &[], bytes.len()).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (outcome, exit) = tokio::join!(
        send(a, &grant, source, 0),
        receive_all(b, &grant, &mut receiving)
    );

    assert!(
        matches!(outcome, AttemptOutcome::SourceChanged),
        "{outcome:?}"
    );
    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Interrupted));
}

#[tokio::test]
async fn a_stream_with_the_wrong_key_never_begins() {
    let dir = TempDir::new().unwrap();
    let (_, source) = source_file(&dir, &content(10, 7)).await;
    let grant = AttemptGrant::mint();
    let impostor = AttemptGrant {
        attempt: grant.attempt,
        key: AttemptGrant::mint().key,
    };
    let (a, b) = pipe();

    let (_, admitted) = tokio::join!(send(a, &impostor, source, 0), admitted(b, &grant));

    assert!(matches!(admitted, Err(BeginError::Frame(_))));
}

#[tokio::test]
async fn a_sender_that_vanishes_leaves_a_correct_prefix() {
    let dir = TempDir::new().unwrap();
    let bytes = content(3 * CHUNK_SIZE, 8);
    let (part, mut receiving) = part_file(&dir, &[], bytes.len()).await;
    let grant = AttemptGrant::mint();
    let (mut a, b) = pipe();

    let vanishing = async {
        a.write_all(grant.attempt.as_bytes()).await.unwrap();
        let (data_encoder, _) = codec::sender(grant.keys(Role::Sender));
        let mut data = FramedWrite::new(a, data_encoder);
        data.send(Outgoing::Begin).await.unwrap();
        data.send(Outgoing::Data(&bytes[..STREAM_FRAME_PAYLOAD]))
            .await
            .unwrap();
    };
    let (_, exit) = tokio::join!(vanishing, receive_all(b, &grant, &mut receiving));

    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Interrupted));
    assert_eq!(receiving.written(), STREAM_FRAME_PAYLOAD as u64);
    assert!(read(&part) == bytes[..STREAM_FRAME_PAYLOAD]);
}

#[tokio::test]
async fn a_receiver_that_vanishes_interrupts_the_sender() {
    let dir = TempDir::new().unwrap();
    let (_, source) = source_file(&dir, &content(3 * CHUNK_SIZE, 9)).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let vanishing = async {
        let mut ends = admitted(b, &grant).await.unwrap();
        ends.incoming.next().await;
    };
    let (outcome, _) = tokio::join!(send(a, &grant, source, 0), vanishing);

    assert!(
        matches!(outcome, AttemptOutcome::Interrupted(_)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn data_past_the_declared_size_is_refused_whole() {
    let dir = TempDir::new().unwrap();
    let (part, mut receiving) = part_file(&dir, &[], 10).await;
    let grant = AttemptGrant::mint();
    let (mut a, b) = pipe();

    let oversending = async {
        a.write_all(grant.attempt.as_bytes()).await.unwrap();
        let (read, write) = a.split();
        let (data_encoder, report_decoder) = codec::sender(grant.keys(Role::Sender));
        let mut data = FramedWrite::new(write, data_encoder);
        let mut reports = FramedRead::new(read, report_decoder);
        data.send(Outgoing::Begin).await.unwrap();
        data.send(Outgoing::Data(&[1; 6])).await.unwrap();
        data.send(Outgoing::Data(&[2; 6])).await.unwrap();
        reports.next().await
    };
    let (report, exit) = tokio::join!(oversending, receive_all(b, &grant, &mut receiving));

    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Violation));
    assert_eq!(report.unwrap().unwrap(), Report::Failed(Failure::Violation));
    assert_eq!(read(&part), vec![1; 6]);
}

#[tokio::test]
async fn a_frame_that_does_not_authenticate_interrupts_without_blame() {
    let dir = TempDir::new().unwrap();
    let (part, mut receiving) = part_file(&dir, &[], 10).await;
    let grant = AttemptGrant::mint();
    let (mut a, b) = pipe();

    let injecting = async {
        a.write_all(grant.attempt.as_bytes()).await.unwrap();
        let (read, write) = a.split();
        let (data_encoder, report_decoder) = codec::sender(grant.keys(Role::Sender));
        let mut data = FramedWrite::new(write, data_encoder);
        let mut reports = FramedRead::new(read, report_decoder);
        data.send(Outgoing::Begin).await.unwrap();
        let (mut stranger, _) = codec::sender(AttemptGrant::mint().keys(Role::Sender));
        let mut forged = BytesMut::new();
        stranger
            .encode(Outgoing::Data(&[9; 4]), &mut forged)
            .unwrap();
        let mut write = data.into_inner();
        write.write_all(&forged).await.unwrap();
        reports.next().await
    };
    let (report, exit) = tokio::join!(injecting, receive_all(b, &grant, &mut receiving));

    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Interrupted));
    assert!(report.is_none(), "{report:?}");
    assert!(read(&part).is_empty());
}

#[tokio::test]
async fn progress_that_goes_backwards_is_a_violation() {
    let dir = TempDir::new().unwrap();
    let (_, source) = source_file(&dir, &content(3 * CHUNK_SIZE, 10)).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let regressing = async {
        let mut ends = admitted(b, &grant).await.unwrap();
        for _ in 0..2 {
            assert!(matches!(
                ends.incoming.next().await,
                Some(Ok(Incoming::Data(_)))
            ));
        }
        ends.reports
            .send(Report::Progress(2 * CHUNK_SIZE as u64))
            .await
            .unwrap();
        ends.reports
            .send(Report::Progress(CHUNK_SIZE as u64))
            .await
            .unwrap();
        ends
    };
    let (outcome, _ends) = tokio::join!(send(a, &grant, source, 0), regressing);

    assert!(
        matches!(outcome, AttemptOutcome::ProtocolViolation),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn cancelling_stops_both_sides() {
    let dir = TempDir::new().unwrap();
    let bytes = content(3 * CHUNK_SIZE, 11);
    let (_, source) = source_file(&dir, &bytes).await;
    let grant = AttemptGrant::mint();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (a, _b) = pipe();

    let outcome = run_send_attempt(a, &grant, source, 0, &AtomicU64::new(0), |_| {}, &cancel).await;
    assert!(outcome.is_none(), "{outcome:?}");

    let (_, mut receiving) = part_file(&dir, &[], bytes.len()).await;
    let (mut a, b) = pipe();
    a.write_all(grant.attempt.as_bytes()).await.unwrap();
    let (data_encoder, _) = codec::sender(grant.keys(Role::Sender));
    let mut data = FramedWrite::new(a, data_encoder);
    data.send(Outgoing::Begin).await.unwrap();
    let mut ends = admitted(b, &grant).await.unwrap();
    let exit = receive(&mut ends, &mut receiving, |_| {}, async || true, &cancel).await;
    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Cancelled));
}

async fn scripted_sender(mut stream: Pipe, grant: &AttemptGrant, script: Vec<Outgoing<'_>>) {
    stream.write_all(grant.attempt.as_bytes()).await.unwrap();
    let (data_encoder, _) = codec::sender(grant.keys(Role::Sender));
    let mut data = FramedWrite::new(stream, data_encoder);
    for frame in script {
        data.send(frame).await.unwrap();
    }
}

#[tokio::test]
async fn every_byte_without_end_is_not_committed() {
    let dir = TempDir::new().unwrap();
    let (_, mut receiving) = part_file(&dir, &[], 10).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (_, exit) = tokio::join!(
        scripted_sender(a, &grant, vec![Outgoing::Begin, Outgoing::Data(&[1; 10])]),
        receive_all(b, &grant, &mut receiving)
    );

    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Interrupted));
}

#[tokio::test]
async fn end_before_every_byte_is_a_violation() {
    let dir = TempDir::new().unwrap();
    let (_, mut receiving) = part_file(&dir, &[], 10).await;
    let grant = AttemptGrant::mint();
    let (a, b) = pipe();

    let (_, exit) = tokio::join!(
        scripted_sender(
            a,
            &grant,
            vec![Outgoing::Begin, Outgoing::Data(&[1; 4]), Outgoing::End]
        ),
        receive_all(b, &grant, &mut receiving)
    );

    assert_eq!(exit, ReceiveExit::Unfinished(Unfinished::Violation));
}
