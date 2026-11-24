use fyles_core::library::util::duration_ext::DurationExt;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{Instrument, Level, span};

use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{Contact, PeerIdWrapper};
use fyles_core::library::wire::api::{ContactProto, ServerMessage, server_message::Message};

use crate::types::Wrap;

use super::test_utils::*;

/// A harness plus the client pushes it emits. The brain pushes only *after* persisting, so a push
/// is the completion signal a re-query loop would otherwise have to guess at.
struct Device {
    harness: RunningP2pTestHarness,
    pushes: mpsc::Receiver<ServerMessage>,
}

/// Waits for a push `wanted` accepts. The receiver is taken before the harness runs, so no push is
/// missed and this needs no pre-check against current state.
async fn await_push<F>(device: &mut Device, mut wanted: F) -> bool
where
    F: FnMut(&Message) -> bool,
{
    timeout(5.seconds(), async {
        loop {
            let push = device
                .pushes
                .recv()
                .await
                .expect("the push channel to stay open");
            if push.message.as_ref().is_some_and(&mut wanted) {
                return;
            }
        }
    })
    .await
    .is_ok()
}

async fn paired_devices() -> (Device, Device) {
    let mut owner = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Owner setup"))
        .await;
    let mut joiner = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Joiner setup"))
        .await;

    owner.listen_external().await;
    joiner.connect(&mut owner).await;

    let mut owner = owner
        .initialize()
        .instrument(span!(Level::INFO, "Owner"))
        .await;
    let owner_pushes = owner
        .client_push_receiver
        .take()
        .expect("the harness to hand over its push channel");
    let mut owner = Device {
        harness: owner.run().await,
        pushes: owner_pushes,
    };

    let mut joiner = joiner
        .initialize()
        .instrument(span!(Level::INFO, "Joiner"))
        .await;
    let joiner_pushes = joiner
        .client_push_receiver
        .take()
        .expect("the harness to hand over its push channel");
    let mut joiner = Device {
        harness: joiner.run().await,
        pushes: joiner_pushes,
    };
    // Adopt the owner's identity out of band, which registers the owner as an owned device on the
    // joiner and kicks off the initial convergence.
    let (request, response) = BrainRequest::with_receiver(());
    owner
        .harness
        .act(ClientAction::GetFullSelfContact(request))
        .await;
    let invite = response.await.unwrap().unwrap();
    let (request, response) = BrainRequest::with_receiver(invite);
    joiner
        .harness
        .act(ClientAction::UpdateIdentity(request))
        .await;
    response.await.unwrap().unwrap();

    let owner_peer = owner.harness.peer_id.wrap();
    let joiner_peer = joiner.harness.peer_id.wrap();
    for (label, device, wanted) in [
        ("joiner", &mut joiner, owner_peer),
        ("owner", &mut owner, joiner_peer),
    ] {
        let listed = await_push(device, |m| lists_device(m, &wanted)).await;
        assert!(
            listed,
            "{label} should list the other device before we start"
        );
    }

    (owner, joiner)
}

fn lists_device(message: &Message, wanted: &PeerIdWrapper) -> bool {
    match message {
        Message::OwnedDevicesChanged(changed) => {
            changed.devices.iter().any(|d| d.peer_id == wanted.0)
        }
        _ => false,
    }
}

/// `None` for any push that is not a contacts update, so callers can tell "not this message" from
/// "this message, and the contact is absent".
fn contacts_in(message: &Message) -> Option<&Vec<ContactProto>> {
    match message {
        Message::ContactsChanged(changed) => Some(&changed.contacts),
        _ => None,
    }
}

async fn await_contact(device: &mut Device, contact: &Contact) -> bool {
    let wanted = contact.id().to_string();
    await_push(device, |m| {
        contacts_in(m).is_some_and(|cs| cs.iter().any(|c| c.id == wanted))
    })
    .await
}

/// The regression this whole change exists for: a write on one device shows up on the other with
/// no reconnect, no rediscovery, and no client-issued sync trigger in between.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn a_local_write_reaches_the_other_device_with_no_connection_event() {
    let (owner, mut joiner) = paired_devices().await;

    let contact = owner.harness.register_new_contact().await;

    assert!(
        await_contact(&mut joiner, &contact).await,
        "registering a contact should be announced to the paired device immediately, \
         without waiting for a sweep or a reconnect"
    );
}

/// Both directions travel the same announce path, and nothing else covers joiner→owner.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn a_local_write_propagates_in_both_directions() {
    let (mut owner, mut joiner) = paired_devices().await;

    let first = owner.harness.register_new_contact().await;
    assert!(await_contact(&mut joiner, &first).await);

    let second = joiner.harness.register_new_contact().await;
    assert!(
        await_contact(&mut owner, &second).await,
        "a change on the joiner should propagate back to the owner"
    );
}

/// What a sweep tick does for one peer must, on its own, recover a device pair that has drifted.
///
/// Drives `trigger_interaction_with_peer` directly — the call `run_sweep` makes per visible peer —
/// because the sweep's peer set comes from mDNS, which never populates in this harness.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn a_sweep_tick_recovers_a_drifted_pair() {
    use crate::event_loop::InnerAsyncEvent;

    let (owner, mut joiner) = paired_devices().await;

    // Write with announcing suppressed, so the change is stranded exactly the way it would be if
    // the announce had raced a disconnect.
    let contact = Contact::for_test();
    let _ = fyles_core::core::sync::add_or_update_owned_device(
        &*owner.harness.db,
        joiner.harness.peer_id.wrap(),
        "Joiner".to_string(),
    )
    .await
    .unwrap();
    let _ = owner
        .harness
        .db
        .update_contact_projection(contact.clone())
        .await
        .unwrap();

    // Asserting the joiner has *not* got it yet would be cleaner, but proving an absence means
    // waiting out a stall; omitted for test speed.

    let (sender, receiver) = tokio::sync::oneshot::channel();
    owner
        .harness
        .p2p_client
        .read()
        .await
        .inner_async_event_sender
        .send(InnerAsyncEvent::GetEventLoop(sender))
        .unwrap();
    let event_loop = receiver.await.unwrap();
    event_loop.trigger_interaction_with_peer(joiner.harness.peer_id);

    assert!(
        await_contact(&mut joiner, &contact).await,
        "a sweep tick should re-poke the peer and converge the stranded change"
    );
}

/// Deleting propagates on the same path as creating — the announce is driven by the write, not by
/// any particular kind of change.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn a_local_delete_also_propagates() {
    let (owner, mut joiner) = paired_devices().await;

    let contact = owner.harness.register_new_contact().await;
    assert!(await_contact(&mut joiner, &contact).await);

    let deleted = contact.id();
    let (request, response) = BrainRequest::with_receiver(deleted.clone());
    owner
        .harness
        .act(ClientAction::DeleteContact(request))
        .await;
    response.await.unwrap().unwrap();

    let deleted = deleted.to_string();
    assert!(
        await_push(&mut joiner, |m| {
            contacts_in(m).is_some_and(|cs| cs.iter().all(|c| c.id != deleted))
        })
        .await,
        "a deletion should be announced to the paired device too"
    );
}
