require 'logger'
require 'benchmark'

class String
  def self.random(len=32, random=nil, character_set = ["A".."Z", "a".."z", "0".."9"])
    if random
      rand_fun = random.method(:rand)
    else
      rand_fun = method(:rand)
    end

    characters = character_set.map { |i| i.to_a }.flatten
    characters_len = characters.length
    (0...len).map{ characters[rand_fun.call(characters_len)] }.join
  end
end

class Array
  def sum
    self.inject 0, :+
  end
end

module Utilities
  def log
    return @logger unless @logger.nil?
    @logger = Log4r::Logger.new("Mosql::Measurements")
    outputter = Log4r::StdoutOutputter.new(STDERR)
    outputter.formatter = Log4r::PatternFormatter.new(
      :pattern => "%d [%l]: %M",
      :date_pattern => "%Y-%m-%d %H:%M:%S.%L")

    @logger.outputters = outputter

    # Sigh, hack to try to get mosql logger to work
    MoSQL::CLI.new(['-v', '-v']).parse_args
    @logger
  end

  def random(seed=99)
    if @random.nil? || seed != @oldseed
      @random ||= Random.new seed
    end
    @random
  end

  def random_record
    {
      :title => String.random(10, random),
      :nested => {
        :int => random.rand(10000000)
      }
    }
  end

  def random_operation_generator(total, probabilities={})
    # A random operation, either :insert, :update, :delete
    ops = [:insert, :update, :delete]

    p = {}
    ops.each do |op|
      p[op] = probabilities[op] if probabilities[op]
    end

    left = 1.0 - p.values.sum
    if p.count != 3
      ops.each { |op| p[op] ||= (left / p.count) }
    end

    log.debug("Probabilities are: #{p}")
    # fit to ranges
    p[:update] += p[:insert]
    p[:delete] += p[:insert] + p[:update]

    (0...total).each do |i|
      t = random.rand

      if t <= p[:insert]
        yield [i, :insert, random_record]
      elsif t <= p[:update]
        yield [i, :update, random_record]
      else
        yield [i, :delete, nil]
      end
    end
  end

  def measure(log_result=true, &blk)
    results = Benchmark.measure { blk.call }
    if log_result
      log.info(results.format("Took %t seconds (real: %r, user: %u, system: %y)"))
    end
    results
  end

  def measure_rubyprof
    result = RubyProf.profile do 
      yield
    end
    printer = RubyProf::GraphPrinter.new(result)
    printer.print(STDOUT, {})
  end

  def batch(batch_size, total)
    at = 0
    while at < total
      endpoint = [at+batch_size-1, total].min
      yield [at, endpoint]
      at = endpoint+1
    end
  end

  def fork_pool(n_forks, &blk)
    processes = (1..n_forks).map do |i|
      Process.fork { blk.call(i) }
    end

    processes.each do |pid|
      Process.wait(pid)
      log.debug("Child #{pid} exited")
    end
  end
end
