require 'mosql'
require 'mosql/cli'
require 'optparse'
require './utilities'


class MeasureImport
  include Utilities

  attr_reader :mongo_uri, :sql_uri, :n_rows, :options
  def initialize(options)
    @mongo_uri = options[:mongo]
    @sql_uri = options[:sql]
    @n_rows = options[:n_rows]
    @options = options
  end

  def mongo
    @_upstream_client ||= Mongo::MongoClient.from_uri(mongo_uri)
  end

  def sql
    @_upstream_sql ||= MoSQL::SQLAdapter.new(schema, sql_uri)
  end

  def schema
    @_schema ||= MoSQL::Schema.new(YAML.load_file('collection.yaml'))
  end

  def create_collection!(collection)
    log.info("Populating mongo collection with #{n_rows} objects")

    @object_ids = []
    measure do
      batch(50000, n_rows) do |start, endpoint|
        log.debug("Batch inserting/generating [#{start}...#{endpoint}]")
        @objects = (start..endpoint).map { |n| random_record }
        collection.insert(@objects)
      end
    end
  end

  def setup_mosql
    metadata_table = MoSQL::Tailer.create_table(sql.db, 'mosql_tailers')

    tailer = MoSQL::Tailer.new([mongo], :existing, metadata_table,
                                :service => "measurements-import")

    streamer = MoSQL::Streamer.new(
                :options =>{:reimport => true},
                :tailer  => tailer,
                :mongo   => mongo,
                :sql     => sql,
                :schema  => schema)
  end

  def mosql_import!
    # largely copied from cli.rb in mosql
    streamer = setup_mosql
    sql.db.drop_table?('blog_posts')

    log.info("Mosql setup done, importing")
    measure do
      streamer.import
    end
  end

  def run!
    config = mongo['admin'].command(:ismaster => 1)
    unless config['setName']
      log.warn("#{mongo_uri} is not a replset!")
    end

    collection = mongo['test_mosql_measurements']['test_collection']

    log.info("Current size of collection is #{collection.size}.")
    if collection.size != n_rows || options[:recreate]
      collection.remove()
      create_collection!(collection)
      log.info("Collection size is now #{collection.size}")
    end

    mosql_import!
  end

  def self.initialize_from_argv
    options = {
      :sql    => 'postgres:///',
      :mongo  => 'mongodb://localhost',
      :n_rows   => 1000000,
      :reimport => false
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

      opts.on("-n [n_rows]", "Number of rows to create") do |n|
        options[:n_rows] = n.to_i
      end

      opts.on("--recreate", "Recreate mongo collection (when doing import benchmark)") do
        options[:recreate] = true
      end
    end

    optparse.parse!

    MeasureImport.new(options)
  end
end

if __FILE__ == $0
  measure = MeasureImport.initialize_from_argv
  measure.run!
end