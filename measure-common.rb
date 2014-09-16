require 'mosql'
require 'mosql/cli'
require 'optparse'
require './utilities'


class MeasureCommon
  include Utilities

  attr_reader :options
  def mongo
    @_upstream_client ||= Mongo::MongoClient.from_uri(options[:mongo])
  end

  def sql
    @_upstream_sql = MoSQL::SQLAdapter.new(schema, options[:sql])
  end

  def schema
    @_schema ||= MoSQL::Schema.new(YAML.load_file('collection.yaml'))
  end

  def collection(ns=nil)
    unless ns.nil?
      db, coll = ns.split(".")
      @_collections ||= {}
      return @_collections[ns] ||= mongo[db][coll]
    end
    @_collection ||= mongo['test_mosql_measurements']['test_collection']
  end

  def setup_mosql
    sql.db.drop_table?('mosql_tailers')
    metadata_table = MoSQL::Tailer.create_table(sql.db, 'mosql_tailers')

    tailer = MoSQL::Tailer.new([mongo], :existing, metadata_table,
                                :service => "measurements-import")

    streamer = MoSQL::Streamer.new(
      :options =>{:reimport => true},
      :tailer  => tailer,
      :mongo   => mongo,
      :sql     => sql,
      :schema  => schema)

    log.info("Mosql setup done")
    [streamer, tailer]
  end

  def self.initialize_from_argv
    options = {
      :sql     => 'postgres:///',
      :mongo   => 'mongodb://localhost',
      :processes => 3
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

      opts.on("-n [rows]", "Number of rows/oplog entries to create") do |n|
        options[:rows] = n.to_i
      end

      opts.on("-t [processes]", "How many processes to use for populating mongo") do |n|
        options[:processes] = n.to_i
      end

      opts.on("--recreate", "Recreate mongo collection/oplog") do
        options[:recreate] = true
      end
    end

    optparse.parse!

    self.new(options)
  end
end