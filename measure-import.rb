require './measure-common'

class MeasureImport < MeasureCommon
  attr_reader :n_rows
  def initialize(options)
    @n_rows = options[:rows] || 1000000
    @options = options
  end

  def create_collection!
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
    streamer = setup_mosql[0]
    sql.db.drop_table?('blog_posts')

    log.info("Mosql setup done, importing")
    measure do
      streamer.import
    end
  end

  def run!
    config = mongo['admin'].command(:ismaster => 1)
    unless config['setName']
      log.warn("#{mongo} is not a replset!")
    end

    log.info("Current size of collection is #{collection.size}.")
    if collection.size != n_rows || options[:recreate]
      collection.remove()
      create_collection!
      log.info("Collection size is now #{collection.size}")
    end

    mosql_import!
  end
end

if __FILE__ == $0
  measure = MeasureImport.initialize_from_argv
  measure.run!
end