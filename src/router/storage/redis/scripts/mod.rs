//! Operation scripts are assembled with their shared helpers at compile time.
//! Each invocation remains one atomic Redis script with its own cached SHA.

use std::sync::LazyLock;

use ::redis::Script;

macro_rules! script {
    ($name:ident, $($file:literal),+ $(,)?) => {
        pub(super) static $name: LazyLock<Script> = LazyLock::new(|| {
            Script::new(concat!($(include_str!($file), "\n",)+))
        });
    };
}

script!(NEXT_ID, "next_id.lua");
script!(CREATE_RESERVATION, "create_reservation.lua");
script!(READ, "common.lua", "read.lua");
script!(
    PUT_TRANSACTION,
    "common.lua",
    "transaction.lua",
    "put_transaction.lua"
);
script!(
    TRY_CLAIM_EXECUTION,
    "common.lua",
    "lease.lua",
    "try_claim_execution.lua"
);
script!(
    TRY_CLAIM_RECOVERY,
    "common.lua",
    "lease.lua",
    "try_claim_recovery.lua"
);
script!(
    MARK_EXECUTION_DISPATCHING,
    "common.lua",
    "lease.lua",
    "mark_execution_dispatching.lua"
);
script!(
    CANCEL_EXECUTION,
    "common.lua",
    "lease.lua",
    "cancel_execution.lua"
);
script!(
    COMPLETE_EXECUTION,
    "common.lua",
    "lease.lua",
    "transaction.lua",
    "complete_execution.lua"
);
