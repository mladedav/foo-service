use std::num::NonZeroU8;

pub fn is_local(this_instance: u8, instance_count: NonZeroU8, shard: i16) -> bool {
    #[expect(clippy::manual_range_contains, reason = "The suggestion is ugly.")]
    if shard > 256 || shard < 0 {
        // We allow `u16` just because it's simpler for callers which have this value from DB.
        tracing::error!(shard, "Invalid shard.");
        return false;
    }

    let this_instance = u16::from(this_instance);
    let instance_count = u16::from(instance_count.get());
    let shard = u16::try_from(shard).expect("we have already checked that it is in 0..256");

    let base_count = 256 / instance_count;
    let instances_with_additional = 256 % instance_count;

    let first = this_instance * base_count + this_instance.min(instances_with_additional);
    let mut count = base_count;
    if instances_with_additional > this_instance {
        count += 1;
    }
    let last = first + count;

    shard >= first && shard < last
}
