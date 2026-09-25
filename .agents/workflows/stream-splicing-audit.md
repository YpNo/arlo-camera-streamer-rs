# Workflow: Stream Splicing Audit
**Role**: Senior Media Quality Engineer

## Objective
Verify that the transition from the "Idle" dummy stream to the "Live" Arlo feed is within latency targets and does not cause RTSP client disconnects.

## Triggers
- Modification of `crates/streamer-infra-media/src/splice.rs`.
- Changes to GStreamer pipeline descriptions in `crates/streamer-infra-media/src/pipeline_desc.rs`.
- Updates to `arlo-rs` that affect stream startup time.

## Steps

### 1. Instrumentation Check
Ensure that the following spans are active and capturing timestamps:
- `stream_request_latency`: Time from trigger to first live packet.
- `splice_duration`: Time to swap sources in the GStreamer pipeline.

### 2. Integration Testing
Run the integration tests with `GST_DEBUG` enabled to inspect pipeline transitions:
```bash
GST_DEBUG=2 rtk cargo test -p streamer-infra-media --test splice_tests
```

### 3. Log Analysis
Check the logs for "Idle-to-Live" transition events:
- Verify that "Requesting Live" is followed by "Live Active".
- Ensure no "EOS" (End of Stream) messages are sent to the RTSP sink during splicing.

### 4. Visual Verification (Manual)
If running in an environment with a display or using `filesink`:
- Record a transition and verify there is no significant frame drop or artifacting during the switch.

## Success Criteria
- [ ] Transition latency (trigger to live) < 5 seconds.
- [ ] Zero RTSP client disconnects during splicing.
- [ ] No "broken pipeline" errors on the GStreamer bus.
