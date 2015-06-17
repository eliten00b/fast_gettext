module FastGettext
  class ClusterCache
    def initialize
      @store = {}
      reload!
      get_remote_timestamp!
      update_remote_timestamp! if @remote_timestamp.nil?
      @local_timestamp = @remote_timestamp
      update_ttl!
    end

    def fetch(key)
      reload! if translations_timestamp_changed?
      translation = @current[key]
      if translation.nil? # uncached
        @current[key] = yield || false # TODO get rid of this false hack and cache :missing
      else
        translation
      end
    end

    # TODO only used for tests, maybe if-else around it ...
    def []=(key, value)
      @current[key] = value
    end

    # key performance gain:
    # - no need to lookup locale on each translation
    # - no need to lookup text_domain on each translation
    # - super-simple hash lookup
    def switch_to(text_domain, locale)
      @store[text_domain] ||= {}
      @store[text_domain][locale] ||= {}
      @store[text_domain][locale][""] = false # ignore gettext meta key when translating
      @current = @store[text_domain][locale]
    end

    def delete(key)
      reload!
      update_remote_timestamp!
      true
    end

    def reload!
      @local_timestamp = Time.now.to_i
      @current = {}
      @current[""] = false
    end

    def self.set_etcd_uri(address)
      @@address = address
    end


    private

    def update_ttl!
      @ttl = Time.now + 60.seconds
    end

    def get_remote_timestamp!
      @remote_timestamp = begin
        response = etcd_http.request Net::HTTP::Get.new(etcd_uri.request_uri)
        if response.code == "200"
          JSON.parse(response.body)['node']['value'].to_i
        elsif response.code == "404"
          nil
        else
          false
        end
      rescue Errno::ECONNREFUSED
        false
      end
    end

    def update_remote_timestamp!
      begin
        request = Net::HTTP::Put.new(etcd_uri.request_uri)
        request.set_form_data({:value => @local_timestamp})
        etcd_http.request request
      rescue Errno::ECONNREFUSED
      end
    end

    def etcd_uri
      @etcd_uri ||= begin
        raise 'Use FastGettext::ClusterCache.set_etcd_uri first!' unless @@address
        URI.parse(@@address)
      end
    end

    def etcd_http
      @etcd_http || begin
        @etcd_http = Net::HTTP.new etcd_uri.host, etcd_uri.port
        @etcd_http.read_timeout = 10
        @etcd_http
      end
    end

    def translations_timestamp_changed?
      return false if @local_timestamp == false || @remote_timestamp == false
      return false if Time.now < @ttl
      update_ttl!
      get_remote_timestamp!
      return false unless @remote_timestamp # false or nil
      return false unless @remote_timestamp > @local_timestamp
      true
    end
  end
end
