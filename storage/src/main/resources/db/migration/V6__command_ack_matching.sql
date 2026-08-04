-- What is needed to match a vehicle's answer back to the command that caused it.
--
-- MAVLink COMMAND_ACK carries: command (the MAV_CMD id), result, progress,
-- result_param2, target_system, target_component. It carries NO correlation id.
-- The vehicle never sees this platform's command id and cannot echo it back, so
-- an ACK can only be matched on (device, MAV_CMD id).
--
-- That is not unique on its own. ARM and DISARM are both MAV_CMD 400, told apart
-- only by param1, so an ACK for 400 with two of them outstanding is ambiguous --
-- and guessing would credit a disarm to an arm, or report a vehicle armed when it
-- had just been disarmed.
--
-- The fix is to make the ambiguity impossible rather than to resolve it: at most
-- one unanswered command per (tenant, device, MAV_CMD id). A second one is
-- refused at issue time, by the database, so a concurrent pair cannot slip
-- through a check-then-act in application code.

ALTER TABLE command ADD COLUMN mav_command_id INTEGER;

-- Backfill from what the existing rows' types map to. The set is small and closed.
UPDATE command SET mav_command_id = CASE command_type
    WHEN 'ARM'              THEN 400
    WHEN 'DISARM'           THEN 400
    WHEN 'TAKEOFF'          THEN 22
    WHEN 'LAND'             THEN 21
    WHEN 'RETURN_TO_LAUNCH' THEN 20
END;

ALTER TABLE command ALTER COLUMN mav_command_id SET NOT NULL;

-- Partial, because only unanswered commands can be confused for one another. Once
-- a command is ACKED, REJECTED or TIMED_OUT its answer has already been accounted
-- for and the next command of the same type is free to go.
CREATE UNIQUE INDEX command_one_outstanding_idx
    ON command (tenant_id, device_id, mav_command_id)
    WHERE state IN ('PENDING', 'SENT');

-- The matching query: oldest unanswered command for this device and MAV_CMD id.
CREATE INDEX command_ack_match_idx
    ON command (tenant_id, device_id, mav_command_id, created_at)
    WHERE state IN ('PENDING', 'SENT');
