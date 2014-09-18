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
  attr_reader :oplog_size, :child_id, :processes, :options
  def initialize(options, child_id=0)
    @oplog_size = options[:rows]
    @processes = options[:processes]
    @child_id = child_id
    @options = options
    log.info(options)
  end

  def oplog
    @_oplog = mongo['local']['oplog.rs']
  end

  def populate_oplog!(size, ops=nil)
    # probabilities = {:insert => 0.9, :update => 0, :delete => 0.1}

    # small chance of them collinding, but oh well.
    #collection.find({}, :limit => 10).to_a.map { |r| r['_id'].to_s }.to_set
    treshold = 0

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
        id = collection.insert(record).to_s
        alive_ids << id
        # log.debug("INSERT #{record}, #{id}")
      when :update
        id = alive_ids.take(1).first
        collection.update({"_id" => BSON::ObjectId(id)}, record)
        # log.debug("UPDATE #{record}, #{id}")
      when :delete
        id = alive_ids.take(1).first
        alive_ids.delete(id)
        collection.remove({"_id" => BSON::ObjectId(id)})
        # log.debug("DELETE #{record}, #{id}")
      end

      # log.info("Filling oplog of #{ns} with #{size} done")
    end
  end

  def create_oplog_parallel!
    log.debug("Oplog size: #{oplog.size}")
    if oplog.size != oplog_size || options[:recreate]
      oplog.remove
      log.info "Populating oplog with #{oplog_size} records."
      measure do
        # fork_pool(processes) do |i|
        #   m = MeasureTail.new(options.dup, i)
        #   m.random(i+99)

        #   # m.log.info("child #{i} started.")
        #   m.populate_oplog!(oplog_size / processes, [[:insert, 1000], [:update, 1]])
        #   m.log.info("child #{i} finished.")
        # end
        populate_oplog!(oplog_size, [[:insert, 20], [:update, 1]])
      end
      log.info("Oplog created.")
    end
  end

  def run!
    # largely copied from streamer.rb

    #last_timestamp = Mongoriver::Tailer.new([mongo], :existing).latest_oplog_entry["ts"]
    # log.info("End time: #{last_timestamp}")

    streamer, tailer = setup_mosql
    # streamer.import

    create_oplog_parallel!
    #tailer.tail()

    # log.info(sql.db[:blog_posts1],a)
    sql.db.drop_table? "blog_posts#{child_id}_backup"
    sql.db.run "CREATE TABLE blog_posts#{child_id}_backup AS TABLE blog_posts#{child_id};"

    log.info("Starting tailing #{child_id}")

    @_last_op = nil
    # has_more = true
    # measure do
    measure_rubyprof("output/#{options[:outfile]}#{child_id}") do
      begin
        streamer.optail
      rescue Exception => e
        log.error(e)
      end
      #while has_more
        # has_more = tailer.stream(1000) do |op|
        #   @_last_op = op
        #   streamer.handle_op(op)
        #   # log.debug(op)
        # end
        # t = Time.at(@_last_op["ts"].seconds).utc
        # behind = last_timestamp - t
        # log.debug("Behind #{behind} seconds. #{has_more}")
      #end
    end
    # end

    log.info("Dropping changes!")
    sql.db.drop_table "blog_posts#{child_id}"
    sql.db.rename_table("blog_posts#{child_id}_backup", "blog_posts#{child_id}")
  end
end



if __FILE__ == $0
  measure = MeasureTail.initialize_from_argv
  # measure.create_oplog_parallel!
  measure.run!
end