require './measure-tail'
fork_pool((1..1)) do |i|
  measure = MeasureTail.initialize_from_argv(i)
  measure.create_oplog_parallel!
  measure.run!
end