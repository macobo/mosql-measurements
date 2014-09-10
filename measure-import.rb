require './measure-common'

class MeasureImport < MeasureCommon
  include Utilities

  attr_reader :mongo_uri, :sql_uri, :n_rows, :options
  def initialize(options)
    @mongo_uri = options[:mongo]
    @sql_uri = options[:sql]
    @n_rows = options[:n_rows]
    @options = options
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
end

if __FILE__ == $0
  measure = MeasureImport.initialize_from_argv
  measure.run!
end