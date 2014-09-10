require 'mosql'
require 'optparse'
require './utilities'


class MeasureImport
  include Utilities

  attr_reader :mongo_uri, :sql_uri, :n_rows
  def initialize(mongo_uri, sql_uri, n_rows)
    @mongo_uri = mongo_uri
    @sql_uri = sql_uri
    @n_rows = n_rows
  end

  def mongo
    @_upstream_client ||= Mongo::MongoClient.from_uri(mongo_uri)
  end

  def run!
    config = mongo['admin'].command(:ismaster => 1)
    unless config['setName']
      log.warn("#{mongo_uri} is not a replset!")
    end


  end

  def self.initialize_from_argv
    options = {
      :sql    => 'postgres:///',
      :mongo  => 'mongodb://localhost',
      :rows   => 1000000
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

      opts.on("-n [rows]", "Number of rows to create") do |n|
        options[:rows] = n.to_i
      end
    end

    optparse.parse!

    MeasureImport.new(options[:mongo], options[:sql], options[:rows])
  end
end

if __FILE__ == $0
  measure = MeasureImport.initialize_from_argv
  measure.run!
end