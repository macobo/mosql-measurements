require './measure-common'
require 'mongoriver'
require 'set'

# Strategy here is to assume the data has been created and imported by mosql.
# The following steps are:
# 1. delete current oplog (so each next time we would be measuring the same things)
# 2. populate oplog with random operations.
# 3. Measure how log optail takes to catch up
# (4. Check for correctness)
class MeasureTail < MeasureCommon
  attr_reader :oplog_size, :child_id, :processes

  NAMESPACES = ["test_mosql_measurements.test_collection", "test_mosql_measurements.test_collection2"]
  TABLES = ["blog_posts", "blog_posts2"]

  def initialize(options, child_id=0)
    @oplog_size = options[:rows]
    @processes = options[:processes]
    @child_id = child_id
    @options = options
  end

  def oplog
    @_oplog = mongo['local']['oplog.rs']
  end

  def populate_oplog!(size, namespaces, ops=nil)
    # probabilities = {:insert => 0.9, :update => 0, :delete => 0.1}

    # small chance of them collinding, but oh well.
    #collection.find({}, :limit => 10).to_a.map { |r| r['_id'].to_s }.to_set
    treshold = 0

    namespaces.each do |ns|
      alive_ids = Set.new
      operation_generator(size, ops) do |at, op, record|
      #random_operation_generator(size, probabilities) do |at, op, record|
        if at > treshold
          p = 100.0 * (at - 1) / size
          # log.debug("Child #{child_id} is #{p}% done.")
          treshold = (p + 20) / 100.0 * size
        end

        case op
        when :insert
          id = collection(ns).insert(record).to_s
          alive_ids << id
          # log.debug("INSERT #{record}, #{id}")
        when :update
          id = alive_ids.take(1).first
          collection(ns).update({"_id" => BSON::ObjectId(id)}, record)
          # log.debug("UPDATE #{record}, #{id}")
        when :delete
          id = alive_ids.take(1).first
          alive_ids.delete(id)
          collection(ns).remove({"_id" => BSON::ObjectId(id)})
          # log.debug("DELETE #{record}, #{id}")
        end
      end

      log.info("Filling oplog of #{ns} with #{size} done")
    end
  end

  def run_cli
    args = ['--mongo', @options[:mongo], "--sql", @options[:sql],
            '--collections', 'collection.yaml', '--threaded']
    MoSQL::CLI.run(args)
  end

  def run!
    log.debug("Oplog size: #{oplog.size}")
    if oplog.size != oplog_size || options[:recreate]
      oplog.remove
      log.info "Populating oplog with #{oplog_size} records."
      measure do
        fork_pool(processes) do |i|
          m = MeasureTail.new(options.dup, i)
          m.random(i+99)

          # m.log.info("child #{i} started.")
          m.populate_oplog!(
            oplog_size / processes,
            NAMESPACES,
            [[:insert, 10], [:update, 10], [:delete, 10]])
          m.log.info("child #{i} finished.")
        end
      end
      log.info("Oplog created.")
    end

    # largely copied from streamer.rb

    # last_timestamp = Mongoriver::Tailer.new([mongo], :existing).latest_oplog_entry["ts"]
    # log.info("End time: #{last_timestamp}")


    # streamer, tailer = setup_mosql
    # tailer.tail()


    TABLES.each do |t|
      sql.db.drop_table? "#{t}_backup"
      sql.db.run "CREATE TABLE #{t}_backup AS TABLE #{t};"
    end

    log.info("Starting tailing")

    @_last_op = nil
    # has_more = true
    #measure do
    measure_rubyprof("output/tail-parallel") do
      run_cli
      # while has_more
      #   has_more = tailer.stream(1000) do |op|
      #     @_last_op = op
      #     streamer.handle_op(op)
      #     # log.debug(op)
      #   end
      #   t = Time.at(@_last_op["ts"].seconds).utc
      #   behind = last_timestamp - t
      #   log.debug("Behind #{behind} seconds. #{has_more}")
      # end
    end


    log.info("Dropping changes!")
    TABLES.each do |t|
      sql.db.drop_table? t
      sql.db.rename_table(t+"_backup", t)
    end
  end
end

if __FILE__ == $0
  unless ARGV.first == 'slave'
    measure = MeasureTail.initialize_from_argv
    measure.run!
  end
end