//! Client contract for one shared seed.
//!
//! The server publishes a 16-character hex seed. Each cat's private material is
//! `SHA-256(seed_hex ‖ cat_id)` with the hex text, not the decoded bytes.
//! Life-event weights run on the phone. A loot claim steps that digest
//! `action_nonce` times and checks a fixed window.

use sha2::{Digest, Sha256};

pub const FULLNESS_CAP: i32 = 100;
pub const DRAIN_MIN: i32 = 1;
pub const DRAIN_MAX: i32 = 5;

/// Handbook event for a claimed treasure pickup.
pub const LUCKY_ITEM_EVENT: &str = "LUCKY_ITEM_PICKUP";

/// Hits when `roll % 100` is below this. The handbook leaves the odds open.
pub const LUCKY_ITEM_PERCENT: u64 = 5;

/// Upper bound on `action_nonce`. A huge nonce would pin a worker on hashes.
pub const MAX_ACTION_NONCE: u64 = 10_000;

/// Per-cat salt. `global_seed` is the 16 hex characters from the SSE payload.
pub fn personal_seed(global_seed: &str, cat_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(global_seed.as_bytes());
    hasher.update(cat_id.as_bytes());
    hasher.finalize().into()
}

fn roll_u64(material: &[u8], tag: &str, counter: u64) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(material);
    hasher.update(tag.as_bytes());
    hasher.update(counter.to_be_bytes());
    let digest = hasher.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 is 32 bytes"))
}

/// Hash `personal` exactly `steps` times and read the first 8 bytes.
pub fn stepped_roll(personal: &[u8; 32], steps: u64) -> u64 {
    let mut state = *personal;
    for _ in 0..steps {
        let mut hasher = Sha256::new();
        hasher.update(state);
        state = hasher.finalize().into();
    }
    u64::from_be_bytes(state[..8].try_into().expect("sha256 is 32 bytes"))
}

/// Satiety lost this minute, in `1..=5`. Same inputs always return the same loss.
pub fn drain_amount(global_seed: &str, cat_id: &str, minute: u64) -> i32 {
    let span = (DRAIN_MAX - DRAIN_MIN + 1) as u64;
    let personal = personal_seed(global_seed, cat_id);
    (roll_u64(&personal, "drain", minute) % span) as i32 + DRAIN_MIN
}

pub fn apply_drain(fullness: i32, amount: i32) -> i32 {
    fullness.saturating_sub(amount).max(0)
}

/// True when the `action_nonce`-th step lands in the loot window.
pub fn lucky_hit(global_seed: &str, cat_id: &str, nonce: u64) -> bool {
    let personal = personal_seed(global_seed, cat_id);
    stepped_roll(&personal, nonce) % 100 < LUCKY_ITEM_PERCENT
}

pub fn valid_seed(seed: &str) -> bool {
    seed.len() == 16
        && seed
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifeEvent {
    Food,
    Poop,
    Wake,
}

impl LifeEvent {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "FOOD_EVENT" => Some(Self::Food),
            "POOP_EVENT" => Some(Self::Poop),
            "WAKE_EVENT" => Some(Self::Wake),
            _ => None,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Food => "FOOD_EVENT",
            Self::Poop => "POOP_EVENT",
            Self::Wake => "WAKE_EVENT",
        }
    }

    /// Weights after 食 / 痾 / 瞓. Totals are 100.
    fn weights(self) -> &'static [(&'static str, u32)] {
        match self {
            Self::Food => &[("sleep", 50), ("groom", 30), ("walk", 15), ("zoomies", 5)],
            Self::Poop => &[("zoomies", 60), ("walk", 25), ("groom", 10), ("sleep", 5)],
            Self::Wake => &[("hungry", 60), ("walk", 25), ("zoomies", 10), ("groom", 5)],
        }
    }
}

/// Pick one behaviour for a life event. `counter` is the client's step since
/// this seed, so blinks and steps do not repeat the same draw.
pub fn behaviour(global_seed: &str, cat_id: &str, event: LifeEvent, counter: u64) -> &'static str {
    let personal = personal_seed(global_seed, cat_id);
    let weights = event.weights();
    let total: u32 = weights.iter().map(|(_, weight)| *weight).sum();
    let mut cursor = (roll_u64(&personal, event.tag(), counter) % u64::from(total)) as u32;
    for (name, weight) in weights {
        if cursor < *weight {
            return name;
        }
        cursor -= weight;
    }
    weights.last().map(|(name, _)| *name).unwrap_or("idle")
}

pub fn valid_cat_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cats_do_not_share_a_personal_seed() {
        let seed = "0123456789abcdef";
        assert_ne!(personal_seed(seed, "cat-a"), personal_seed(seed, "cat-b"));
    }

    #[test]
    fn drain_stays_in_range_and_kills_at_zero() {
        let seed = "0123456789abcdef";
        let amount = drain_amount(seed, "momo", 10);
        assert!((DRAIN_MIN..=DRAIN_MAX).contains(&amount));
        assert_eq!(apply_drain(2, 5), 0);
        assert_eq!(apply_drain(100, amount), 100 - amount);
    }

    #[test]
    fn behaviour_is_one_of_the_weighted_actions() {
        let seed = "0123456789abcdef";
        let food = ["sleep", "groom", "walk", "zoomies"];
        let action = behaviour(seed, "momo", LifeEvent::Food, 0);
        assert!(food.contains(&action));
        assert_eq!(action, behaviour(seed, "momo", LifeEvent::Food, 0));
    }

    #[test]
    fn lucky_hit_depends_on_the_nonce_and_repeats() {
        let seed = "0123456789abcdef";
        assert_eq!(lucky_hit(seed, "momo", 245), lucky_hit(seed, "momo", 245));
        let personal = personal_seed(seed, "momo");
        let once = stepped_roll(&personal, 1);
        let twice_state = {
            let mut hasher = Sha256::new();
            hasher.update(personal);
            let next: [u8; 32] = hasher.finalize().into();
            stepped_roll(&next, 0)
        };
        assert_eq!(once, twice_state);
        assert!(valid_seed(seed));
        assert!(!valid_seed("0123456789ABCDE"));
        assert!(!valid_seed("0123456789ABCDEF"));
    }

    #[test]
    fn cat_ids_are_key_safe() {
        assert!(valid_cat_id("momo_1"));
        assert!(!valid_cat_id(""));
        assert!(!valid_cat_id("a/b"));
        assert!(!valid_cat_id(&"x".repeat(65)));
    }
}
