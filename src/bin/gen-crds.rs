use kube::CustomResourceExt;
use postgres_restore_operator::types::{PostgresPhysicalReplica, PostgresPhysicalRestore};

fn main() {
    let replica_crd = serde_json::to_value(PostgresPhysicalReplica::crd()).unwrap();
    let restore_crd = serde_json::to_value(PostgresPhysicalRestore::crd()).unwrap();

    let docs = vec![replica_crd, restore_crd];
    for (i, doc) in docs.iter().enumerate() {
        if i > 0 {
            print!("---\n");
        }
        print!("{}", serde_yaml::to_string(doc).unwrap());
    }
}
