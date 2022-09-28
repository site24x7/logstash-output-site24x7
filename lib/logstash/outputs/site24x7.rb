require "logstash/outputs/base"
require "logstash/json"
require "zlib"
require "date"
require "json"
require "base64"

class LogStash::Outputs::Site24x7 < LogStash::Outputs::Base
  config_name "site24x7"

  S247_MAX_RECORD_COUNT = 500
  S247_MAX_RECORD_SIZE = 1000000
  S247_MAX_BATCH_SIZE = 5000000
  S247_LOG_UPLOAD_CHECK_INTERVAL = 600 #10 minutes
  S247_TRUNCATION_SUFFIX = "##TRUNCATED###"

  default :codec, "json"

  config :log_type_config,:validate => :string, :required => true
  config :log_source,:validate => :string, :required => false, :default => Socket.gethostname
  config :max_retry, :validate => :number, :required => false,  :default => 3
  config :retry_interval, :validate => :number, :required => false, :default => 2
	
  public
  def register
    init_variables()
    init_http_client(@logtype_config)
  end # def register


  public
  def multi_receive(events)
    return if events.empty?
    process_http_events(events)
  end

  def close
      @s247_http_client.close if @s247_http_client
  end

  def base64_url_decode(str)
     str += '=' * (4 - str.length.modulo(4))
     Base64.decode64(str.tr('-_','+/'))
  end
  
  def init_variables()
    @logtype_config = JSON.parse(base64_url_decode(@log_type_config))
    @s247_custom_regex = if @logtype_config.has_key? 'regex' then Regexp.compile(@logtype_config['regex'].gsub('?P<','?<')) else nil end
    @s247_ignored_fields = if @logtype_config.has_key? 'ignored_fields' then @logtype_config['ignored_fields'] else [] end
    @s247_tz = {'hrs': 0, 'mins': 0} #UTC
    @valid_logtype = true
    @log_upload_allowed = true
    @log_upload_stopped_time = 0
    @s247_datetime_format_string = @logtype_config['dateFormat']
    @s247_datetime_format_string = @s247_datetime_format_string.sub('%f', '%N')
    if !@s247_datetime_format_string.include? 'unix'
      @is_year_present = if @s247_datetime_format_string.include?('%y') || @s247_datetime_format_string.include?('%Y') then true else false end
      if !@is_year_present
	@s247_datetime_format_string = @s247_datetime_format_string+ ' %Y'
      end   
      @is_timezone_present = if @s247_datetime_format_string.include? '%z' then true else false end
      if !@is_timezone_present && @logtype_config.has_key?('timezone')
	tz_value = @logtype_config['timezone']
	if tz_value.start_with?('+')
	    @s247_tz['hrs'] = Integer('-' + tz_value[1..4])
	    @s247_tz['mins'] = Integer('-' + tz_value[3..6])
	elsif tz_value.start_with?('-')
	    @s247_tz['hrs'] = Integer('+' + tz_value[1..4])
	    @s247_tz['mins'] = Integer('+' + tz_value[3..6])
	end
      end
    end
  end

  def init_http_client(logtype_config)
    require 'manticore'
    @upload_url = 'https://'+logtype_config['uploadDomain']+'/upload'
    @logger.info("Starting HTTP connection to #{@upload_url}")
    @headers = {"Content-Type" => "application/json", "Content-Encoding" => "gzip", "X-DeviceKey" => logtype_config['apiKey'], "X-LogType" => logtype_config['logType'], "X-StreamMode" => "1", "User-Agent" => "LogStash"}
    @s247_http_client = Manticore::Client.new({})
  end

  def get_timestamp(datetime_string)
    begin
        # If the date value is in unix format the no need to process the date string
        if @s247_datetime_format_string.include? 'unix'
            return (if @s247_datetime_format_string == 'unix' then datetime_string+'000' else datetime_string end)
        end
        datetime_string += if !@is_year_present then ' '+String(Time.new.year) else '' end
        if !@is_timezone_present && @logtype_config.has_key?('timezone')
            @s247_datetime_format_string += '%z'
            time_zone = String(@s247_tz['hrs'])+':'+String(@s247_tz['mins'])
            datetime_string += if time_zone.start_with?('-') then time_zone else '+'+time_zone end
        end
        datetime_data = DateTime.strptime(datetime_string, @s247_datetime_format_string)
        return Integer(datetime_data.strftime('%Q'))
    rescue
        return 0
    end
  end

  def parse_lines(lines)
    parsed_lines = []
    log_size = 0
    lines.each do |line|
       if !line.empty?
	    begin
		if match = line.match(@s247_custom_regex)
                    log_size += line.bytesize
		    log_fields = match&.named_captures
		    removed_log_size=0
		    @s247_ignored_fields.each do |field_name|
		        removed_log_size += if log_fields.has_key?field_name then log_fields.delete(field_name).bytesize else 0 end
		    end
		    formatted_line = {'_zl_timestamp' => get_timestamp(log_fields[@logtype_config['dateField']]), 's247agentuid' => @log_source}
		    formatted_line.merge!(log_fields)
                    parsed_lines.push(formatted_line)
		    log_size -= removed_log_size
                else
                    @logger.debug("pattern not matched regex : #{@s247_custom_regex} and received line : #{line}")
		end
	    rescue Exception => e
		@logger.error("Exception in parse_line #{e.backtrace}")
	    end
       end
    end
    return parsed_lines, log_size
  end

  def is_filters_matched(formatted_line)
    if @logtype_config.has_key?'filterConfig'
        @logtype_config['filterConfig'].each do |config|
            if formatted_line.has_key?config && (filter_config[config]['match'] ^ (filter_config[config]['values'].include?formatted_line[config]))
                return false
	    end
        end
    end
    return true
  end

  def get_json_value(obj, key, datatype=nil)
    if obj != nil && (obj.has_key?key)
       if datatype and datatype == 'json-object'
	  arr_json = []
          child_obj = obj[key]
          if child_obj.class == String
             child_obj = JSON.parse(child_obj.gsub('\\','\\\\'))
          end             
          child_obj.each do |key, value|
             arr_json.push({'key' => key, 'value' => String(value)})
          end
          return arr_json
       else
         return (if obj.has_key?key then obj[key] else obj[key.downcase] end)
       end
    elsif key.include?'.'
	parent_key = key[0..key.index('.')-1]
	child_key = key[key.index('.')+1..-1]
        child_obj = obj[if obj.has_key?parent_key then parent_key else parent_key.capitalize() end]
	if child_obj.class == String
            child_obj = JSON.parse(child_obj.replace('\\','\\\\'))
        end
        return get_json_value(child_obj, child_key)
    end
  end

  def json_log_parser(lines_read)
    log_size = 0
    parsed_lines = []
    lines_read.each do |line|
        if !line.empty?
          current_log_size = 0
	  formatted_line = {}
	  event_obj = Yajl::Parser.parse(line)
	  @logtype_config['jsonPath'].each do |path_obj|
	    value = get_json_value(event_obj, path_obj[if path_obj.has_key?'key' then 'key' else 'name' end], path_obj['type'])
            if value
	      formatted_line[path_obj['name']] = value 
	      current_log_size+= String(value).size - (if value.class == Array then value.size*20 else 0 end)
            end
          end
	  if is_filters_matched(formatted_line)
	    formatted_line['_zl_timestamp'] = get_timestamp(formatted_line[@logtype_config['dateField']])
	    formatted_line['s247agentuid'] = @log_source
	    parsed_lines.push(formatted_line)
            log_size+=current_log_size
          end
	end
    end
    return parsed_lines, log_size
  end
  
  def process_http_events(events)
    batches = batch_http_events(events)
    batches.each do |batched_event|
      formatted_events, log_size = format_http_event_batch(batched_event)
      formatted_events = gzip_compress(formatted_events)
      send_logs_to_s247(formatted_events, log_size)
    end
  end

  def batch_http_events(encoded_events)
    batches = []
    current_batch = []
    current_batch_size = 0
    encoded_events.each_with_index do |encoded_event, i|
      event_message = encoded_event.to_hash['message']
      current_event_size = event_message.bytesize
      if current_event_size > S247_MAX_RECORD_SIZE
        event_message = event_message[0..(S247_MAX_RECORD_SIZE-DD_TRUNCATION_SUFFIX.length)]+DD_TRUNCATION_SUFFIX
        current_event_size = event_message.bytesize
      end

      if (i > 0 and i % S247_MAX_RECORD_COUNT == 0) or (current_batch_size + current_event_size > S247_MAX_BATCH_SIZE)
        batches << current_batch
        current_batch = []
        current_batch_size = 0
      end

      current_batch_size += current_event_size
      current_batch << event_message
    end
    batches << current_batch
    batches
  end

  def format_http_event_batch(events)
    parsed_lines = []
    log_size = 0
    if @logtype_config.has_key?'jsonPath'
      parsed_lines, log_size = json_log_parser(events)
    else
       parsed_lines, log_size = parse_lines(events)
    end
    return LogStash::Json.dump(parsed_lines), log_size
  end

  def gzip_compress(payload)
    gz = StringIO.new
    gz.set_encoding("BINARY")
    z = Zlib::GzipWriter.new(gz, 9)
    begin
      z.write(payload)
    ensure
      z.close
    end
    gz.string
  end

  def send_logs_to_s247(gzipped_parsed_lines, log_size)
     @headers['Log-Size'] = String(log_size)
     sleep_interval = @retry_interval
     begin
        @max_retry.times do |counter|
          need_retry = false
          begin
            response = @s247_http_client.post(@upload_url, body: gzipped_parsed_lines, headers: @headers).call
            resp_headers = response.headers.to_h
            if response.code == 200
              if resp_headers.has_key?'LOG_LICENSE_EXCEEDS' && resp_headers['LOG_LICENSE_EXCEEDS'] == 'True'
                @logger.error("Log license limit exceeds so not able to send logs")
                @log_upload_allowed = false
		@log_upload_stopped_time =Time.now.to_i
              elsif resp_headers.has_key?'BLOCKED_LOGTYPE' && resp_headers['BLOCKED_LOGTYPE'] == 'True'
                @logger.error("Max upload limit reached for log type")
                @log_upload_allowed = false
		@log_upload_stopped_time =Time.now.to_i
              elsif resp_headers.has_key?'INVALID_LOGTYPE' && resp_headers['INVALID_LOGTYPE'] == 'True'
                @logger.error("Log type not present in this account so stopping log collection")
		@valid_logtype = false
              else
		@log_upload_allowed = true
                @logger.debug("Successfully sent logs with size #{gzipped_parsed_lines.size} / #{log_size} to site24x7. Upload Id : #{resp_headers['x-uploadid']}")
              end
            else
              @logger.error("Response Code #{response.code} from Site24x7, so retrying (#{counter + 1}/#{@max_retry})")
              need_retry = true
            end
          rescue StandardError => e
            @logger.error("Error connecting to Site24x7. exception: #{e.backtrace}")
          end

          if need_retry
            if counter == @max_retry - 1
              @logger.error("Could not send your logs after #{max_retry} tries")
              break
            end
            sleep(sleep_interval)
            sleep_interval *= 2
          else
            return
          end
        end
      rescue Exception => e
        @logger.error("Exception occurred in sendig logs : #{e.backtrace}")
      end
  end

end
