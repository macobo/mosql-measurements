require 'mosql'
require 'mosql/cli'
require 'optparse'
require './utilities'


class MeasureCommon
  include Utilities

  def mongo
    @_upstream_client ||= Mongo::MongoClient.from_uri(mongo_uri)
  end

  def sql
    @_upstream_sql ||= MoSQL::SQLAdapter.new(schema, sql_uri)
  end

  def schema
    @_schema ||= MoSQL::Schema.new(YAML.load_file('collection.yaml'))
  end

  def setup_mosql
    metadata_table = MoSQL::Tailer.create_table(sql.db, 'mosql_tailers')

    tailer = MoSQL::Tailer.new([mongo], :existing, metadata_table,
                                :service => "measurements-import")

    MoSQL::Streamer.new(
      :options =>{:reimport => true},
      :tailer  => tailer,
      :mongo   => mongo,
      :sql     => sql,
      :schema  => schema)
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

      opts.on("--recreate", "Recreate mongo collection (import benchmark)") do
        options[:recreate] = true
      end
    end

    optparse.parse!

    self.new(options)
  end
end