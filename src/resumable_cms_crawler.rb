# use Mechanize to crawl and parse
# use MongoDB to store data
# use Parallel to crawl in multiple threads

require 'rubygems'
require 'mechanize'
require 'fastercsv'
require 'parallel'
require 'mongo'

CONN = Mongo::Connection.new
DB   = CONN['healthcare_sg']
COLL = DB['urls']
RESULT_COLL = DB['results']

URL_PREFIX = "http://www.smc.gov.sg"
FORM_URL = "http://www.smc.gov.sg/PRSCPDS/scripts/profSearch/search.jsp"
SEARCH_URL = "http://www.smc.gov.sg/PRSCPDS/profsearch?param=part"

=begin
  here is the core place retrieving information we need
  with regular expression, css selector, html selector or xPath
=end
def parse_single_page(page)
  rows =  page.search("form[name='search'] table > tr > td > table > tr")
  
  dentist_name = ''
  company_name = ''
  address = ''
  
  rows.each do |row|
    plain = row.text.gsub(/\s*\302\240\302\240/, '').gsub(/\s\s/, '').strip
    if plain.start_with?("Name")
      dentist_name = plain.gsub(/Name/, '')
    end
    if plain.start_with?("Primary Practice Place")
      company_name = plain.gsub(/Primary Practice Place/, '')
    end
    if plain.start_with?("Practice Address")
      address = row.text.gsub(/\s\s/, '').gsub(/\302\240\302\240/, ' ').gsub(/Practice Address/, '').strip
    end
  end
  p [dentist_name, company_name, address]

  RESULT_COLL.insert(:contact => [dentist_name, company_name, address])
end

=begin
  parse the search result page, usually with nav paging 
=end
def parse_result_page(page)
  page.search("div.listing div.title a").map do |result_row|
    result_row.attribute("href").value
  end
end

=begin
  some sites are stateful, which stores something in your cookies
  here you can manipulate it, or 
  create the cookies naturally by visiting the website in specific sequences, or even with login & password
=end
def touch_and_customize_cookies
  agent = Mechanize.new
  # retrieve the jsessionid and init the cookies
  begin
    agent.get(FORM_URL)
  rescue Mechanize::ResponseCodeError => e  
  end

  agent.get(SEARCH_URL)
  agent.get("http://www.smc.gov.sg/PRSCPDS/scripts/profSearch/searchList.jsp?page=0&spectext=")
  
  agent
end

=begin
  launcher to parse the search result page, usually with nav paging 
=end
def craw_result_page_urls
  start_time = Time.now
  agent = touch_and_customize_cookies
  
  detail_urls = []
  page_indexes = (0..245)

  Parallel.each(page_indexes, :in_threads => 2, :in_processes => 2) do |page_index|
  # (0..245).each do |page_index|
    url = "http://www.smc.gov.sg/PRSCPDS/scripts/profSearch/searchList.jsp?page=#{page_index}&spectext="  
    search_results = agent.post(url)
    search_results.search(".displayTabledata a").each do |link|
      COLL.insert(:url => link.attribute("href").value)
    end
  end
  p Time.now - start_time
end

=begin
  launcher to access individual detail pages
  it only visits those haven't been parsed
=end
def craw_contacts
  start_time = Time.now

  agent = touch_and_customize_cookies
  
  values = COLL.find({"done" => {"$exists" => false}})
  
  Parallel.each(values, :in_threads => 2, :in_processes => 2) do |value|
  # values.each do |value|
    detail_url = value['url']
    parse_single_page(agent.get(URL_PREFIX + detail_url))
    COLL.update({"_id" => value['_id']}, {"$set" => {"done" => true}})
  end

  p Time.now - start_time
end

=begin
  launcher to export all information into csv file, with header
=end
def export_contacts_to_csv
  start_time = Time.now
  
  contacts = RESULT_COLL.find()
  
  FasterCSV.open("../output/healthcare_sg_export.csv", 'w') {|csv|
    csv <<  ["contact_name", "company_name", "address"]
    contacts.each do |row|
      csv << row["contact"]
    end
  }
  p Time.now - start_time
end

# craw_result_page_urls
# craw_contacts
export_contacts_to_csv