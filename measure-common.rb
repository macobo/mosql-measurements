require 'mosql'
require 'mosql/cli'
require 'optparse'
require './utilities'


class MeasureCommon
  include Utilities

  attr_reader :options
  def mongo
    @_upstream_client ||= Mongo::MongoClient.from_uri(options.fetch(:mongo))
  end

  def sql
    @_upstream_sql ||= MoSQL::SQLAdapter.new(schema, options.fetch(:sql))
  end

  def schema
    @_schema ||= MoSQL::Schema.new(YAML.load_file("collection#{child_id}.yaml"))
  end

  def collection(ns=nil)
    unless ns.nil?
      db, coll = ns.split(".")
      @_collections ||= {}
      return @_collections[ns] ||= mongo[db][coll]
    end
    @_collection ||= mongo['test_mosql_measurements']["test_collection#{child_id}"]
  end

  def setup_mosql
    # sql.db.drop_table?('mosql_tailers')
    metadata_table = MoSQL::Tailer.create_table(sql.db, 'mosql_tailers')

    tailer = MoSQL::Tailer.new([mongo], :existing, metadata_table,
                                :service => "measurements-import")

    tailer.log.level = Log4r::DEBUG

    streamer = MoSQL::Streamer.new(
      :options =>{:reimport => true},
      :tailer  => tailer,
      :mongo   => mongo,
      :sql     => sql,
      :schema  => schema)

    log.info("Mosql setup done")
    [streamer, tailer]
  end

  def self.initialize_from_argv(child_id=0)
    options = {
      :sql     => 'postgres:///',
      :mongo   => 'mongodb://localhost',
      :processes => 3,
      :outfile => self.to_s,
      :child_id => child_id
    }
    optparse = OptionParser.new do |opts|
      opts.banner = "Usage: #{$0} [options] "

      opts.on('-h', '--help', "Display this message") do
        puts opts
        exit(0)
      end

      opts.on("--sql [sqluri]", "SQL server to connect to") do |uri|
        options[:sql] = uri
      end

      opts.on("--mongo [mongouri]", "Mongo connection string") do |uri|
        options[:mongo] = uri
      end


      opts.on("--outfile [filename]", "Mongo connection string") do |filename|
        options[:outfile] = filename
      end

      opts.on("-n [rows]", "Number of rows/oplog entries to create") do |n|
        options[:rows] = n.to_i
      end

      opts.on("-t [processes]", "How many processes to use for populating mongo") do |n|
        options[:processes] = n.to_i
      end

      opts.on("--recreate", "Recreate mongo collection/oplog") do
        options[:recreate] = true
      end


      opts.on("--child-id [x]", "Recreate mongo collection/oplog") do |n|
        options[:child_id] = n.to_i
      end
    end

    optparse.parse!(ARGV.dup)

    self.new(options, options[:child_id])
  end
end