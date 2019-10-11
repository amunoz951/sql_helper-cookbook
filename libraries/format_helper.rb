require 'time'

module FormatHelper

  module_function

  module DateTime
    module_function

    def yyyymmdd(date = Time.now)
      date = ::Time.parse(date) unless date.is_a?(Time)
      date.strftime('%Y%m%d')
    end

    def hhmmss(time = Time.now)
      time = ::Time.parse(time) unless time.is_a?(Time)
      time.strftime('%H%M%S')
    end

    def yyyymmdd_hhmmss(date_and_time = Time.now)
      date_and_time = ::Time.parse(date_and_time) unless date_and_time.is_a?(Time)
      date_and_time.strftime('%Y%m%d_%H%M%S')
    end
  end

  module File

    module_function

    def windows_friendly_name(name, type: :file) # type options: :file or :path
      type == :file ? name.gsub(%r{[\x00/\\:\*\?\"<>\|]}, '_') : name.gsub(%r{[\x00\*\?\"<>\|]}, '_')
    end
  end

  module Directory

    module_function

    def ensure_trailing_slash(directory)
      return nil if directory.nil?
      ::File.join(directory, '')
    end
  end

  module String

    module_function

    def hex_to_bin(h)
      return nil if h.nil?
      h.scan(/../).map { |x| x.hex.chr }.join
    end
  end

  module Hash

    module_function

    def safe_value(hash, *keys)
      return nil if hash.nil? || hash[keys.first].nil?
      return hash[keys.first] if keys.length == 1 # return the value if we have reached the final key
      safe_value(hash[keys.shift], *keys) # recurse until we have reached the final key
    end

    def stringify_all_keys(hash)
      stringified_hash = {}
      hash.each do |k, v|
        stringified_hash[k.to_s] = v.is_a?(Hash) ? stringify_all_keys(v) : v
      end
      stringified_hash
    end
  end

  # Deep merge two structures
  def deep_merge(base, override, boolean_or: false)
    if base.nil?
      return base if override.nil?
      return override.is_a?(Hash) ? override.dup : override
    end

    case override
    when nil
      base = base.dup if base.is_a?(Hash)
      base # if override doesn't exist, then simply copy base to it
    when ::Hash
      base = base.dup
      override.each do |src_key, src_value|
        base[src_key] = base[src_key] ? FormatHelper.deep_merge(base[src_key], src_value) : src_value
      end
      base
    when ::Array
      base |= override
      base
    when ::String, ::Integer, ::Time, ::TrueClass, ::FalseClass
      boolean_or ? base || override : override
    else
      throw "Implementation for deep merge of type #{override.class} is missing."
    end
  end

  # returns [rows, columns, width, height]
  module TerminalSize

    module_function

    def all
      require 'io/console'
      IO.console.winsize
    rescue LoadError
      # This works with older Ruby, but only with systems
      # that have a tput(1) command, such as Unix clones.
     [Integer(`tput li`), Integer(`tput co`)]
    end

    def rows
      all.first
    end

    def columns
      all.last
    end
  end

  def terminal_line(filler_character)
    filler = filler_character * (TerminalSize.columns - 1)
  end

  def terminal_header(header_text = '', filler_character: '*')
    filler = filler_character * ((TerminalSize.columns - (header_text.length + 29)) / 2)
    IOHelper.logger.info "#{filler} #{header_text} #{filler}" + filler_character * ((TerminalSize.columns + header_text.length + 1) % 2)
  end

  # Optional parameters should be an array of symbols or strings
  def validate_parameters(method, method_binding, optional_parameters = [])
    method.parameters.each do |parameter|
      parameter_name = parameter.last.to_s
      next if optional_parameters.any? { |o| o.to_s.casecmp(parameter_name) == 0 }
      parameter_value = eval(parameter_name, method_binding)
      raise "#{parameter_name} is a required parameter for #{caller[2][/`.*'/][1..-2]}!" if parameter_value.nil? || (parameter_value.respond_to?(:empty?) && parameter_value.empty?)
    end
  end
end
