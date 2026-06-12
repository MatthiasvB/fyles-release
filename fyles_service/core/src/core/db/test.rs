use std::ops::Deref;
use crate::library::util::epoch::unix_epoch_millis;

use crate::{
    core::{
        brain::action_p2p::NodeInfo,
        db::DbError,
        domain_models::{
            FilerequestAccess, InProgress, InProgressSendStatus, PeerIdWrapper, TransferData,
        },
    },
    library::sqlite::{Sqlite, SqliteConfig},
    mocks::db::MockDb,
};

use super::*;
use rand::RngCore;
use tempfile::{tempdir, TempDir};
use tracing::{debug, instrument};
use crate::core::domain_models::FileInfo;
// --------------------------- Helpers ---------------------------

async fn helper_register_contact<D>(db: &D) -> Contact
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let c = Contact::for_test();
    db.register_contact(c.clone()).await.unwrap();
    c
}

async fn helper_create_public_filerequest<D>(db: &D, title: &str) -> (FylesId, Filerequest)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let id = db
        .create_filerequest(&CreateFilerequest {
            title: title.into(),
            description: "Desc".into(),
            is_active: true,
            access: FilerequestAccess::Public,
        })
        .await
        .unwrap();
    let fr = db.get_filerequest(&id).await.unwrap();
    (id, fr)
}

async fn helper_create_audience_filerequest<D>(
    db: &D,
    title: &str,
    contact_ids: Vec<ContactId>,
) -> (FylesId, Filerequest)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let id = db
        .create_filerequest(&CreateFilerequest {
            title: title.into(),
            description: "Desc".into(),
            is_active: true,
            access: FilerequestAccess::Audience { contact_ids },
        })
        .await
        .unwrap();
    let fr = db.get_filerequest(&id).await.unwrap();
    (id, fr)
}

pub struct DbWrapper {
    pub db: Arc<dyn FilerequestDb>,
    pub _db_dir: Option<TempDir>,
}

#[instrument(skip_all, level = "trace")]
pub async fn setup_test_db(node_keys: Option<NodeInfo>, use_memory_db: bool) -> DbWrapper {
    let (db, temp_dir) = if use_memory_db {
        let db = MockDb::new();
        (Arc::new(db) as Arc<dyn FilerequestDb>, None)
    } else {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let config = SqliteConfig { path: db_path };
        let db = Arc::new(Sqlite::with_config(config));
        let db_clone = db.clone();
        debug!("Running test db");
        tokio::spawn(async move { db_clone.run().await });
        (db as Arc<dyn FilerequestDb>, Some(temp_dir))
    };

    if let Some(node_keys) = node_keys {
        db.store_node_keys(node_keys)
            .await
            .expect("Failed to store node keys");
    };

    DbWrapper {
        db,
        _db_dir: temp_dir,
    }
}

// ---------------------- Consolidated Tests ---------------------

/// Consolidated:
/// - Create public & audience filerequests
/// - Read
/// - Update title + audience expansion
/// - Switch access audience->public and public->audience
/// - List preserves access modes
/// - Delete and ensure gone
pub async fn test_filerequest_crud_and_access<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Audience path
    let c1 = helper_register_contact(&db).await;
    let (aud_id, mut aud_fr) =
        helper_create_audience_filerequest(&db, "Audience 1", vec![c1.id.clone()]).await;
    if let FilerequestAccess::Audience { contact_ids } = &aud_fr.access {
        assert_eq!(contact_ids, &vec![c1.id.clone()]);
    } else {
        panic!("Expected audience");
    }

    // Update (title + add member)
    let c2 = helper_register_contact(&db).await;
    aud_fr.title = "Audience 1 Updated".into();
    aud_fr.access = FilerequestAccess::Audience {
        contact_ids: vec![c1.id.clone(), c2.id.clone()],
    };
    db.update_filerequest(&aud_fr).await.unwrap();
    let aud_fr = db.get_filerequest(&aud_id).await.unwrap();
    if let FilerequestAccess::Audience { contact_ids } = &aud_fr.access {
        assert_eq!(contact_ids.len(), 2);
    }

    // Switch to public
    let mut aud_fr_public = aud_fr.clone();
    aud_fr_public.access = FilerequestAccess::Public;
    db.update_filerequest(&aud_fr_public).await.unwrap();
    assert!(matches!(
        db.get_filerequest(&aud_id).await.unwrap().access,
        FilerequestAccess::Public
    ));

    // Public path -> later switched to audience
    let (pub_id, mut pub_fr) = helper_create_public_filerequest(&db, "Public 1").await;
    let c3 = helper_register_contact(&db).await;
    pub_fr.access = FilerequestAccess::Audience {
        contact_ids: vec![c3.id.clone()],
    };
    db.update_filerequest(&pub_fr).await.unwrap();
    match db.get_filerequest(&pub_id).await.unwrap().access {
        FilerequestAccess::Audience { contact_ids } => {
            assert_eq!(contact_ids, vec![c3.id.clone()]);
        }
        _ => panic!("Expected audience after update"),
    }

    // Second audience mutation: replace previous audience member with a new one (purge old)
    let c4 = helper_register_contact(&db).await;
    let mut fr_second_change = db.get_filerequest(&pub_id).await.unwrap();
    fr_second_change.access = FilerequestAccess::Audience {
        contact_ids: vec![c4.id.clone()],
    };
    db.update_filerequest(&fr_second_change).await.unwrap();
    match db.get_filerequest(&pub_id).await.unwrap().access {
        FilerequestAccess::Audience { contact_ids } => {
            assert_eq!(
                contact_ids,
                vec![c4.id],
                "Old audience member should be purged"
            );
        }
        _ => panic!("Expected audience after second mutation"),
    }

    // List ensures both present and access types intact
    let list = db.get_filerequests().await.unwrap();
    assert_eq!(list.len(), 2);
    let audience_count = list
        .iter()
        .filter(|f| matches!(f.access, FilerequestAccess::Audience { .. }))
        .count();
    assert_eq!(audience_count, 1);

    // Delete one
    db.delete_filerequest(&aud_id).await.unwrap();
    assert!(db.get_filerequest(&aud_id).await.is_err());
}

/// Cascade deletion of received_files when filerequest removed.
pub async fn test_filerequest_deletion<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Create a contact for audience
    let contact = Contact::for_test();
    db.register_contact(contact.clone()).await.unwrap();

    // Create filerequest
    let create_request = CreateFilerequest {
        title: "Test Delete Request".to_string(),
        description: "Test Description".to_string(),
        is_active: true,
        access: FilerequestAccess::Audience {
            contact_ids: vec![contact.id.clone()],
        },
    };
    let fr_id = db.create_filerequest(&create_request).await.unwrap();

    // Create some received files (simulate incoming transfer + completion)
    let transfer_id = FylesId::new();
    let incoming = CreateIncomingFile {
        filerequest_id: fr_id.clone(),
        file_name: "test.txt".to_string(),
        file_size_bytes: 1234,
        transfer_id: transfer_id.clone(),
        contact_id: Some(contact.id.clone()),
        peer_id: "test-peer-id".to_string(),
        started_at_ms: 0,
    };
    db.create_incoming_file(&incoming).await.unwrap();
    db.complete_received_file(&CompleteReceivedFile {
        transfer_id,
        file_path: "/tmp/test.txt".to_string(),
        received_at_ms: 12345,
    })
    .await
    .unwrap();

    // Verify filerequest exists
    let fr = db.get_filerequest(&fr_id).await.unwrap();
    assert_eq!(fr.title, "Test Delete Request");

    // Verify received files exist
    let received_files = db.list_received_files(&fr_id).await.unwrap();
    assert_eq!(received_files.len(), 1);

    // Delete filerequest
    db.delete_filerequest(&fr_id).await.unwrap();

    // Verify filerequest is gone
    assert!(db.get_filerequest(&fr_id).await.is_err());

    // Verify received files are cascade deleted
    let received_files = db.list_received_files(&fr_id).await.unwrap();
    assert_eq!(
        received_files.len(),
        0,
        "Received files should be deleted when filerequest is deleted"
    );
}

/// Combined node key tests (initial absence, integrity for patterned sizes, overwrite).
pub async fn test_node_keys_integrity<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Absence
    assert!(matches!(
        db.get_node_keys().await,
        Err(DbError::DataNotYetInitialized)
    ));

    // Helper to write & verify a key vector
    async fn write_and_check<D>(db: &D, bytes: Vec<u8>)
    where
        D: Deref<Target = dyn FilerequestDb>,
    {
        let node_info = NodeInfo::generate_random_bytes(bytes.clone());
        db.store_node_keys(node_info.clone()).await.unwrap();
        let got = db.get_node_keys().await.unwrap();
        assert_eq!(got.node_key_pair, bytes);
        assert_eq!(got, node_info);
    }

    // Pattern sizes
    for size in [32usize, 64usize] {
        let mut key = vec![0u8; size];
        for i in 0..size {
            key[i] = (i % 256) as u8;
        }
        write_and_check(&db, key.clone()).await;
    }

    // Overwrite with inverse pattern
    let mut rev = vec![0u8; 64];
    for i in 0..64 {
        rev[i] = (255 - i as u16) as u8;
    }
    write_and_check(&db, rev).await;

    // Random final overwrite
    let mut random = [0u8; 64];
    rand::thread_rng().fill_bytes(&mut random);
    write_and_check(&db, random.to_vec()).await;
}

/// Pending files including negative case (invalid remote filerequest).
pub async fn test_pending_files<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Negative first: invalid remote
    let bogus_remote = FylesId::new();
    let bad = db
        .create_pending_files(&CreatePendingFiles {
            file_infos: vec![FileInfo { path: "/x".into(), display_name: Some("x".into()) }],
            target_filerequest_id: bogus_remote.clone(),
        })
        .await;
    assert!(bad.is_err(), "Should fail for unknown remote filerequest");

    // Normal flow
    let peer_id = PeerIdWrapper::for_test();
    let contact = helper_register_contact(&db).await;
    let remote_id = db
        .create_remote_filerequest(&CreateRemoteFilerequest {
            peer_id: peer_id.clone(),
            filerequest_id: "remote-fr-123".into(),
            name: "Remote Test Filerequest".into(),
            contact_id: contact.id.clone(),
        })
        .await
        .unwrap();

    let pending_ids = db
        .create_pending_files(&CreatePendingFiles {
            file_infos: vec![FileInfo { path: "/path/to/file.txt".into(), display_name: Some("file.txt".into()) }],
            target_filerequest_id: remote_id.clone(),
        })
        .await
        .unwrap();
    let first = db.get_pending_file(&pending_ids[0]).await.unwrap();
    assert_eq!(first.status, SendStatus::Pending);

    db.handle_update_pending_file_status(
        &pending_ids[0],
        &SendStatus::InProgress(InProgress {
            status: InProgressSendStatus::Sending,
            transfer_data: TransferData {
                progress_bytes: 0,
                file_size_bytes: 0,
                transfer_id: Default::default(),
            },
        }),
        Some(0),
    )
    .await
    .unwrap();
    assert!(matches!(
        db.get_pending_file(&pending_ids[0]).await.unwrap().status,
        SendStatus::InProgress(_)
    ));

    // Additional pending
    let _ = db
        .create_pending_files(&CreatePendingFiles {
            file_infos: vec![FileInfo { path: "/path/to/another.txt".into(), display_name: Some("another.txt".into()) }],
            target_filerequest_id: remote_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(db.get_all_pending_files().await.unwrap().len(), 2);

    // Cascade delete
    db.delete_remote_filerequest(&remote_id).await.unwrap();
    assert_eq!(db.get_all_pending_files().await.unwrap().len(), 0);
    assert!(db.get_pending_file(&pending_ids[0]).await.is_err());
}

/// Received file insert + list.
pub async fn test_received_files<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Create contact and filerequest first
    let contact = Contact::for_test();

    db.register_contact(contact.clone()).await.unwrap();

    let filerequest_id = db
        .create_filerequest(&CreateFilerequest {
            title: "Test Request".to_string(),
            description: "Test Description".to_string(),
            is_active: true,
            access: FilerequestAccess::Public,
        })
        .await
        .unwrap();

    // Test storing a received file (create incoming, then complete)
    let transfer_id = FylesId::new();
    let incoming = CreateIncomingFile {
        contact_id: Some(contact.id.clone()),
        filerequest_id: filerequest_id.clone(),
        transfer_id: transfer_id.clone(),
        peer_id: "test-peer-id".to_string(),
        file_name: "test.txt".to_string(),
        file_size_bytes: 1234,
        started_at_ms: 0,
    };
    db.create_incoming_file(&incoming).await.unwrap();
    let id = db
        .complete_received_file(&CompleteReceivedFile {
            transfer_id,
            file_path: "/tmp/test.txt".to_string(),
            received_at_ms: unix_epoch_millis().unwrap(),
        })
        .await
        .unwrap();
    let mut files = db.list_received_files(&filerequest_id).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id, id);
    assert_eq!(files[0].contact_id.take().unwrap(), contact.id);
}

/// Delete existing received file.
pub async fn test_delete_received_file<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Create required contact and filerequest first
    let contact = Contact::for_test();
    db.register_contact(contact.clone()).await.unwrap();

    // Create filerequest
    let filerequest_id = db
        .create_filerequest(&CreateFilerequest {
            title: "Test Request".to_string(),
            description: "Test Description".to_string(),
            is_active: true,
            access: FilerequestAccess::Public,
        })
        .await
        .unwrap();

    // Store a received file (create incoming, then complete)
    let transfer_id = FylesId::new();
    let incoming = CreateIncomingFile {
        contact_id: Some(contact.id.clone()),
        filerequest_id: filerequest_id.clone(),
        transfer_id: transfer_id.clone(),
        peer_id: "test-peer-id".to_string(),
        file_name: "test.txt".into(),
        file_size_bytes: 100,
        started_at_ms: 0,
    };
    db.create_incoming_file(&incoming).await.unwrap();
    let file_id = db
        .complete_received_file(&CompleteReceivedFile {
            transfer_id,
            file_path: "/tmp/test.txt".into(),
            received_at_ms: 12345,
        })
        .await
        .unwrap();
    db.delete_received_file(&file_id).await.unwrap();
    assert_eq!(
        db.list_received_files(&filerequest_id).await.unwrap().len(),
        0
    );
}

/// Delete non-existent received file must error.
pub async fn test_delete_nonexistent_received_file<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let result = db.delete_received_file(&"nonexistent".into()).await;
    assert!(result.is_err());
}

/// Contact deletion prunes audience & errors on phantom delete (ignored in adapters where not enforced).
pub async fn test_contact_deletion_cascade<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Two contacts in audience
    let c1 = Contact::for_test();
    let c2 = Contact::for_test();
    db.register_contact(c1.clone()).await.unwrap();
    db.register_contact(c2.clone()).await.unwrap();
    let fr_id = db
        .create_filerequest(&CreateFilerequest {
            title: "Cascade Test".into(),
            description: "Test".into(),
            is_active: true,
            access: FilerequestAccess::Audience {
                contact_ids: vec![c1.id.clone(), c2.id.clone()],
            },
        })
        .await
        .unwrap();
    let fr = db.get_filerequest(&fr_id).await.unwrap();
    match &fr.access {
        FilerequestAccess::Audience { contact_ids } => assert_eq!(contact_ids.len(), 2),
        _ => panic!("Expected audience"),
    }
    db.delete_contact(&c1.id).await.unwrap();
    let fr = db.get_filerequest(&fr_id).await.unwrap();
    if let FilerequestAccess::Audience { contact_ids } = fr.access {
        assert!(!contact_ids.contains(&c1.id));
        assert!(contact_ids.contains(&c2.id));
    }
    db.delete_contact(&c2.id).await.unwrap();
    let fr = db.get_filerequest(&fr_id).await.unwrap();
    if let FilerequestAccess::Audience { contact_ids } = fr.access {
        assert!(contact_ids.is_empty());
    }
    let phantom = Contact::for_test().id;
    let res = db.delete_contact(&phantom).await;
    assert!(res.is_err(), "Expected error for deleting unknown contact");
}

/// Update non-existent filerequest must error.
pub async fn test_update_nonexistent_filerequest<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let phantom = Filerequest {
        id: FylesId::new(),
        title: "Does not exist".into(),
        description: "Ignored".into(),
        is_active: true,
        access: FilerequestAccess::Public,
    };
    let res = db.update_filerequest(&phantom).await;
    assert!(
        res.is_err(),
        "Updating non-existent filerequest should error"
    );
    let all = db.get_filerequests().await.unwrap();
    assert!(!all.iter().any(|f| f.id == phantom.id));
}

/// Remote filerequests query by contact; unknown returns empty.
pub async fn test_remote_filerequests_unknown_contact_query<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let contact = Contact::for_test();
    db.register_contact(contact.clone()).await.unwrap();
    let peer = PeerIdWrapper::for_test();
    let remote_id = db
        .create_remote_filerequest(&CreateRemoteFilerequest {
            peer_id: peer.clone(),
            filerequest_id: "remote-1".into(),
            name: "Remote".into(),
            contact_id: contact.id.clone(),
        })
        .await
        .unwrap();
    let list = db
        .get_remote_filerequests_by_contact(&contact.id)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, remote_id);
    let unknown = Contact::for_test().id;
    let empty = db
        .get_remote_filerequests_by_contact(&unknown)
        .await
        .unwrap();
    assert!(empty.is_empty());
}

/// Public key retrieval: Some for known, None for unknown.
pub async fn test_contact_public_keys<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    let contact = Contact::for_test();
    let unknown = Contact::for_test().id;
    db.register_contact(contact.clone()).await.unwrap();

    let some = db
        .get_contact_public_keys(contact.id.clone())
        .await
        .expect("query ok");
    assert!(
        some.is_some(),
        "Expected Some(public keys) for known contact"
    );

    let none = db
        .get_contact_public_keys(unknown.clone())
        .await
        .expect("query ok for unknown");
    assert!(none.is_none(), "Expected None for unknown contact id");
}

/// DB initialization & self contact lifecycle:
/// - Before node keys: all getters error
/// - After store_node_keys: node keys & self contact accessible
/// - Name update + identity update work
pub async fn test_db_initialization<D>(db: D)
where
    D: Deref<Target = dyn FilerequestDb>,
{
    // Pre-condition: node keys specifically should yield DataNotYetInitialized
    assert!(matches!(
        db.get_node_keys().await,
        Err(DbError::DataNotYetInitialized)
    ));

    // Sqlite returns a generic database error for absent self_contact (row missing),
    // MockDb returns DataNotYetInitialized. We only assert "is_err" for parity.
    assert!(db.get_self_contact().await.is_err());
    assert!(db.get_self_contact_for_display().await.is_err());
    assert!(db.get_sharable_public_self_contact().await.is_err());

    // Store node keys (seeds self_contact in both backends)
    let node_info = NodeInfo::generate_random_bytes(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    db.store_node_keys(node_info.clone()).await.unwrap();

    // Node keys round-trip
    let got_keys = db.get_node_keys().await.unwrap();
    assert_eq!(got_keys.node_key_pair, node_info.node_key_pair);

    // Self contact retrievals
    let sc = db.get_self_contact().await.unwrap();
    assert_eq!(sc.id, node_info.self_contact_id);
    let sc_disp = db.get_self_contact_for_display().await.unwrap();
    assert_eq!(sc_disp.id, sc.id);
    let public_sc = db.get_sharable_public_self_contact().await.unwrap();
    assert_eq!(public_sc.id, sc.id);
    assert_eq!(public_sc.public_keys.ed25519, sc.keys.public.ed25519);

    // Update name
    db.update_self_contact_name("Renamed".into()).await.unwrap();
    let sc_disp2 = db.get_self_contact_for_display().await.unwrap();
    assert_eq!(sc_disp2.name, "Renamed");

    // Identity update (reuse keys, change name again)
    let mut new_sc = sc.clone();
    new_sc.name = "RenamedAgain".into();
    db.update_identity(new_sc).await.unwrap();
    let sc_disp3 = db.get_self_contact_for_display().await.unwrap();
    assert_eq!(sc_disp3.name, "RenamedAgain");
}
