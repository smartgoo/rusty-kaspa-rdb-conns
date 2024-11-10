use kaspa_consensus::config::Config;
use kaspa_consensus::consensus::{
    factory::MultiConsensusManagementStore, storage::ConsensusStorage,
};
use kaspa_consensus::model::stores::selected_chain::SelectedChainStoreReader;
use kaspa_consensus_core::{
    config::ConfigBuilder,
    network::{NetworkId, NetworkType},
};
use kaspad_lib::daemon::get_app_dir;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

fn get_active_consensus_dir(meta_db_dir: PathBuf) -> String {
    let db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(meta_db_dir)
        .with_files_limit(128)
        .build_readonly()
        .unwrap();
    let store = MultiConsensusManagementStore::new(db);
    let active_consensus_dir = store.active_consensus_dir_name().unwrap().unwrap();
    active_consensus_dir
}

fn read_only_conn(active_consensus_dir: PathBuf, config: Arc<Config>) {
    let db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(active_consensus_dir)
        .with_files_limit(128)
        .build_readonly()
        .unwrap();

    let storage = ConsensusStorage::new(db, config);

    println!("\nRead Only Connection:");
    for _ in 1..3 {
        let idx = storage.selected_chain_store.read().get_tip().unwrap();
        println!("{:?}", idx);

        // Read only connection has point in time view of data of when connection is established
        // This loop will constantly return same results
        sleep(Duration::from_millis(5_000));
    }
}

fn read_only_conn_reinit(active_consensus_dir: PathBuf, config: Arc<Config>) {
    println!("\nRead Only Connection - Drop & Reconnect Between Reads:");
    for _ in 1..3 {
        let db = kaspa_database::prelude::ConnBuilder::default()
            .with_db_path(active_consensus_dir.clone())
            .with_files_limit(128)
            .build_readonly()
            .unwrap();

        let storage = ConsensusStorage::new(db, config.clone());

        let idx = storage.selected_chain_store.read().get_tip().unwrap();
        println!("{:?}", idx);

        sleep(Duration::from_millis(5_000));
    }
}

fn secondary_conn(active_consensus_dir: PathBuf, config: Arc<Config>) {
    let db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(active_consensus_dir)
        .with_files_limit(-1)
        .build_secondary(PathBuf::from_str("secondary").unwrap())
        .unwrap();

    let storage = ConsensusStorage::new(db.clone(), config);

    println!("\nSecondary Connection:");
    for _ in 1..3 {
        let idx = storage.selected_chain_store.read().get_tip().unwrap();
        println!("{:?}", idx);

        // Secondary connection has point in time view of data of when connection is established
        // Secondary connection provides functionality "try_catch_up_with_primary"
        // Sleep for 5 seconds, then try_catch_up_with_primary
        // Cant get this working though. TODO
        sleep(Duration::from_millis(5_000));
        let _ = db.clone().try_catch_up_with_primary().unwrap();
    }
}

fn checkpoint(active_consensus_dir: PathBuf, config: Arc<Config>) {
    let db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(active_consensus_dir.clone())
        .with_files_limit(128)
        .build_readonly()
        .unwrap();

    let checkpoint = rocksdb::checkpoint::Checkpoint::new(&db).unwrap();

    let checkpoint_path = "./checkpoint";
    checkpoint.create_checkpoint(checkpoint_path).unwrap();

    let checkpoint_db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(PathBuf::from_str(checkpoint_path).unwrap())
        .with_files_limit(128)
        .build_readonly()
        .unwrap();
    let checkpoint_storage = ConsensusStorage::new(checkpoint_db, config);
    let idx = checkpoint_storage
        .selected_chain_store
        .read()
        .get_tip()
        .unwrap();
    println!("\nCheckpoint:\n{:?}", idx);

    std::fs::remove_dir_all(checkpoint_path).unwrap();
}

fn main() {
    let network_id = NetworkId::new(NetworkType::Mainnet);

    let app_dir = get_app_dir();
    let db_dir = app_dir.join(network_id.to_prefixed()).join("datadir");
    let meta_db_dir = db_dir.join("meta");
    let consensus_dir = db_dir.join("consensus");
    let active_consensus_dir = consensus_dir.join(get_active_consensus_dir(meta_db_dir));

    let config: Arc<kaspa_consensus::config::Config> = Arc::new(
        ConfigBuilder::new(network_id.into())
            .adjust_perf_params_to_consensus_params()
            .build(),
    );

    // Open read only connection
    // Keeps one persistent connection, does not return latest data as it has point in time view
    read_only_conn(active_consensus_dir.clone(), config.clone());

    // Open read only connection
    // Close and reopen during each loop, returns recent data as a result
    // Point in time view is more recent during each reinit of connection
    read_only_conn_reinit(active_consensus_dir.clone(), config.clone());

    // Secondary connection
    // Cannot get try_catch_up_with_primary working
    // TODO research further
    secondary_conn(active_consensus_dir.clone(), config.clone());

    // Create copy of database using RocksDB checkpoint
    // Then connect to checkpoint and read from there
    // Then deletes checkpoint dir
    checkpoint(active_consensus_dir.clone(), config.clone());
}
