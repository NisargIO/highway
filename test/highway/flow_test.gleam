import highway/domain
import highway/flow

pub fn credit_cumulative_ack_and_timeout_test() {
  let first = domain.Cursor("one", 1, 1)
  let second = domain.Cursor("one", 1, 2)
  let credit =
    flow.new(2) |> flow.sent(first, 10, 0) |> flow.sent(second, 20, 5)
  assert !flow.available(credit)
  assert credit.bytes == 30
  assert !flow.timed_out(credit, 9, 10)
  assert flow.timed_out(credit, 10, 10)
  assert flow.ack(credit, domain.Cursor("one", 1, 3))
    == Error(domain.CursorInvalid)
  assert flow.ack(credit, domain.Cursor("other", 1, 1))
    == Error(domain.CursorInvalid)
  let assert Ok(one) = flow.ack(credit, first)
  assert flow.available(one)
  assert one.bytes == 20
  assert flow.ack(one, first) == Ok(one)
  let assert Ok(empty) = flow.ack(one, second)
  assert empty.bytes == 0
  assert flow.ack(empty, first) == Error(domain.CursorInvalid)
  assert !flow.timed_out(empty, 100, 10)
}

pub fn cumulative_ack_preserves_later_epoch_with_lower_offset_test() {
  let old = domain.Cursor("stream", 1, 200)
  let next = domain.Cursor("stream", 3, 1)
  let c = flow.new(2) |> flow.sent(old, 10, 0) |> flow.sent(next, 20, 1)
  let assert Ok(c) = flow.ack(c, old)
  assert c.bytes == 20
  assert !flow.available(flow.Credit(..c, window: 1))
  let assert Ok(c) = flow.ack(c, next)
  assert c.bytes == 0
}
