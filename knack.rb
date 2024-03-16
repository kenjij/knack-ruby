#
# # KnackAPI
#
# Wrapper for Knack REST API
#
# v.20240315
#
require 'json'
require_relative './neko-http'
require_relative './neko-logger'

class KnackAPI
  BASE_URL = 'https://api.knack.com/v1'
  L = Neko::Logger.logger

  attr_reader :k_hdrs, :k_map_objects, :k_map_fields_key, :k_map_fields_label

  def initialize(app_id, api_key)
    @k_hdrs = {
      'X-Knack-Application-Id' => app_id,
      'X-Knack-REST-API-KEY' => api_key,
    }
    @k_map_objects = {}
    @k_map_fields_key = {}
    @k_map_fields_label = {}
  end

  def get_objects
    r = Neko::HTTP.get(BASE_URL + '/objects', nil, k_hdrs)
    unless r[:code] == 200
      L.warn("KnackAPI failed to get objects: #{r[:code]} #{r[:message]}")
      return nil
    end
    begin
      h = JSON.parse(r[:body], symbolize_names: true)
      h[:objects].each do |o|
        @k_map_objects[o[:name]] = o[:key]
      end
      h
    rescue => e
      L.warn("KnackAPI data error: #{e.message}")
      return nil
    end
  end

  def get_fields(obj)
    okey = key_for_obj(obj)
    path = "/objects/#{okey}/fields"
    r = Neko::HTTP.get(BASE_URL + path, nil, k_hdrs)
    unless r[:code] == 200
      L.warn("KnackAPI failed to get #{okey} fields: #{r[:code]} #{r[:message]}")
      return nil
    end
    begin
      h = JSON.parse(r[:body], symbolize_names: true)
      fields_k = {}
      fields_l = {}
      h[:fields].each do |o|
        fields_k[o[:key]] = o[:label]
        fields_l[o[:label]] = o[:key]
      end
      fields_k['id'] = 'id'
      @k_map_fields_key[okey] = fields_k
      @k_map_fields_label[okey] = fields_l
      h
    rescue => e
      L.warn("KnackAPI data error: #{e.message}")
      return nil
    end
  end

  def get_records(obj, sort_field: 'id', filters: nil, label: false)
    okey = key_for_obj(obj)
    path = "/objects/#{okey}/records"
    prms = {
      format: :raw,
      sort_field: key_for_fld(sort_field, okey),
      sort_order: :asc,
      rows_per_page: 1000,
    }
    if Hash === filters
      prms[:filters] = JSON.fast_generate(filters)
    elsif String === filters
      prms[:filters] = filters
    end
    L.debug("KnackAPI getting records with filters: #{prms[:filters]}")
    r = Neko::HTTP.get(BASE_URL + path, prms, k_hdrs)
    unless r[:code] == 200
      L.warn("KnackAPI failed to get #{okey} records: #{r[:code]} #{r[:message]}")
      return nil
    end
    data = JSON.parse(r[:body])
    records = data['records']
    if records && label && k_map_fields_key[okey]
      records.each do |record|
        record.transform_keys! { |k| k_map_fields_key.dig(okey, k) }
      end
    end
    data
  end

  def get_record(id, obj, label: false)
    okey = key_for_obj(obj)
    path = "/objects/#{okey}/records/#{id}"
    L.info("KnackAPI getting a record: #{path}")
    r = Neko::HTTP.get(BASE_URL + path, nil, k_hdrs)
    unless r[:code] == 200
      L.warn("KnackAPI failed to get #{okey} record #{id}: #{r[:code]} #{r[:message]}")
      return nil
    end
    record = JSON.parse(r[:body])
    if record && label && k_map_fields_key[okey]
      record.transform_keys! { |k| k_map_fields_key.dig(okey, k) }
    end
    record
  end

  def post_record(data, obj, label: false)
    okey = key_for_obj(obj)
    path = "/objects/#{okey}/records"
    L.info("KnackAPI posting a record: #{path}")
    r = Neko::HTTP.post_json(BASE_URL + path, data, k_hdrs)
    unless r[:code] == 200
      L.warn("KnackAPI failed to post a record: #{r[:code]} #{r[:message]}")
      return nil
    end
    record = JSON.parse(r[:body])
    if record && label && k_map_fields_key[okey]
      record.transform_keys! { |k| k_map_fields_key.dig(okey, k) }
    end
    record
  end

  def put_record(id, data, obj, label: false)
    okey = key_for_obj(obj)
    path = "/objects/#{okey}/records/#{id}"
    hdrs = k_hdrs.merge({'Content-Type' => 'application/json'})
    case data
    when Array, Hash
      body = JSON.fast_generate(data)
    when String
      body = data
    else
      raise ArgumentError, 'Argument is neither Array, Hash, String'
    end
    L.info("KnackAPI putting a record: #{path}")
    r = Neko::HTTP.new(BASE_URL + path, hdrs).put(body:body)
    unless r[:code] == 200
      L.warn("KnackAPI failed to put a record: #{r[:code]} #{r[:message]}")
      return nil
    end
    record = JSON.parse(r[:body])
    if record && label && k_map_fields_key[okey]
      record.transform_keys! { |k| k_map_fields_key.dig(okey, k) }
    end
    record
  end

  def delete_record(id, obj)
    okey = key_for_obj(obj)
    path = "/objects/#{okey}/records/#{id}"
    L.info("KnackAPI deleting a record: #{path}")
    r = Neko::HTTP.new(BASE_URL + path, k_hdrs).delete
    unless r[:code] == 200
      L.warn("KnackAPI failed to delete a record: #{r[:code]} #{r[:message]}")
      return nil
    end
    JSON.parse(r[:body])
  end

  def key_for_obj(obj)
    if String === obj
      return obj if obj.match?(/^object_\d+$/)
      return k_map_objects[obj]
    elsif Integer === obj
      return "object_#{obj}"
    end
    return nil
  end

  def key_for_fld(fld, obj)
    if String === fld
      return fld if fld.match?(/^(id|field_\d+)$/)
      return k_map_fields_label.dig(key_for_obj(obj), fld)
    elsif Integer === fld
      return "field_#{fld}"
    end
  end
end
