//! What a start of the database does about its role's password. Row: `P.secrets.a-postgres-role-password-is-the-served-value-on-every-start`
//! (Beyond repo): "A Postgres guest that holds a platform-generated password has its database role's password set to that value on every start, so a
//! fork or restore carrying the source's role hash authenticates with its own value and never the source's."
//!
//! A start is a cold start of the supervisor (a boot, a restart, a fork-branch or restore on a cloned volume: the data directory may carry any role
//! hash) or a successor taking over a running postgres (the same process, so the role is whatever this instance last set). A cold primary start runs
//! `post_start`, whose FIRST statement is `ALTER ROLE postgres WITH PASSWORD <served verifier>`; a replica is a read-only standby and runs none.
//! `plan` is called by `supervisor::run` (whether post_start runs) and by `supervisor::build_post_start_script` (whether it opens with the reset).
//! The served verifier is the one the driver hands the guest over the env relay (`beyond-pg-init/src/env_relay.rs`, a verifier or nothing).
//!
//! Liveness partner: `plan`'s ensures is an iff, so a primary cold start is always planned the reset (completeness), and the reset is idempotent
//! (`role_after` of `served` is `served`), so a retry after a failed post_start converges.

use vstd::prelude::*;

verus! {

/// What a start does.
#[derive(Clone, Copy)]
pub struct Plan {
    /// `post_start` runs.
    pub post_start: bool,
    /// Its first statement sets the role's password to the served value.
    pub reset_role: bool,
}

/// The role's password hash after a start that planned `p`, given the one the data directory held and the served one.
pub open spec fn role_after(stored: Option<Seq<u8>>, served: Seq<u8>, p: Plan) -> Option<Seq<u8>> {
    if p.reset_role { Some(served) } else { stored }
}

/// The plan for a start: `replica` is a hot standby, `successor` a handover of a running postgres.
pub fn plan(replica: bool, successor: bool) -> (p: Plan)
    ensures
        p.post_start <==> (!replica && !successor),
        // Every start that runs post_start resets the role first; none that skips it does.
        p.reset_role <==> p.post_start,
{
    let runs = !replica && !successor;
    if cfg!(feature = "mut_start_keeps_stored_role") {
        return Plan { post_start: runs, reset_role: false };
    }
    Plan { post_start: runs, reset_role: runs }
}

/// A primary's cold start, whatever the data directory carried (a fork's or restore's source hash among it), leaves the role with exactly the served
/// value: the source's hash, when it differs, is gone.
pub proof fn a_start_holds_the_served_value(stored: Option<Seq<u8>>, served: Seq<u8>)
    ensures
        ({
            let p = plan_spec(false, false);
            role_after(stored, served, p) == Some(served)
                && (stored is Some && stored->Some_0 != served ==> role_after(stored, served, p) != stored)
        }),
{
}

/// Retrying after a failed start converges: the reset applied twice is the reset applied once.
pub proof fn the_reset_is_idempotent(stored: Option<Seq<u8>>, served: Seq<u8>)
    ensures
        role_after(role_after(stored, served, plan_spec(false, false)), served, plan_spec(false, false))
            == role_after(stored, served, plan_spec(false, false)),
{
}

/// The spec of `plan`'s answer.
pub open spec fn plan_spec(replica: bool, successor: bool) -> Plan {
    let runs = !replica && !successor;
    Plan { post_start: runs, reset_role: runs }
}

} // verus!
