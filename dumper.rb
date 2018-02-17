Bundler.require

require 'curses'
require 'optparse'
require 'parallel'
require 'digest'

options = {}

OptionParser.new do |parser|
  parser.banner = "Usage: dumper.rb [options]"

  parser.on("-a", "--action ACTION", "Action to perform (dump/load).") do |v|
    options[:action] = v
  end
  parser.on("-u", "--user USERNAME", "Username for mysql connection.") do |v|
    options[:username] = v
  end
  parser.on("-p", "--password PASSWORD", "Password for mysql connection.") do |v|
    options[:password] = v
  end
  parser.on("-h", "--host HOST", "Host for mysql connection.") do |v|
    options[:host] = v
  end
  parser.on("-d", "--db DATABASE", "Database for mysql connection.") do |v|
    options[:db] = v
  end
  parser.on("-D", "--dir DIRECTORY", "Directory for backups.") do |v|
    options[:dir] = v
  end
  parser.on("-t", "--threads THREADS", "Number of threads to use.") do |v|
    options[:threads] = v && v.to_i || 2
  end
  parser.on("", "--help", "Show this help message") do ||
    puts parser
    exit
  end
end.parse!

%I(action username password host db dir).each do |key|
  if options[key].nil? || options[key].empty?
    puts "#{key} need to be specified!"
    exit
  end
end
unless %w(dump load).include?(options[:action])
  puts 'Bad action specified!'
  exit
end
unless Dir.exist?(options[:dir])
  puts "Specified directory doesn't exist"
  exit
end

mysql_e = `which mysql`.strip
mysqldump_e = `which mysqldump`.strip
if mysql_e.empty?
  puts 'mysql executable need to be installed'
  exit
end
if mysqldump_e.empty?
  puts 'mysqldump executable need to be installed'
  exit
end

args = "-u#{options[:username]} -p#{options[:password]} -h#{options[:host]} #{options[:db]}"

def log(msg)
  File.write("#{__dir__}/sql_dumper.log", "[#{Time.now}] #{msg}\n", { mode: 'a' })
end

def place_string(y, x, string)
  Curses.setpos(y, x)
  Curses.addstr(string)
end

def table_status(table, status)
  place_string(
      $table_coords[table][:y],
      $table_coords[table][:x],
      "#{table}#{status.rjust($table_coords[table][:length] - table.size)}")
end

def colonize_tables(tables)
  cols = []
  cols_left = {0 => 0}
  tables.each_slice(Curses.lines - 2) {|e| cols << e}

  $table_coords = {}
  cols.each_with_index do |vals, col|
    cols_left[col + 1] = cols_left[col] + vals.map(&:size).max + 4

    vals.each_with_index do |table, y|
      $table_coords[table] = { x: cols_left[col], y: y + 1, length: cols_left[col + 1] - cols_left[col] - 1 }
    end
  end
end

if options[:action] == 'dump'
  tables = `#{mysql_e} #{args} --skip-column-names -e 'show tables;' 2>/dev/null`.split("\n").map(&:strip)

  if tables.size == 0
    puts "No tables to dump"
    exit
  end

  log("Starting dump to #{options[:dir]} of database #{options[:db]}. Total #{tables.size} to dump.")

  Curses.init_screen
  Curses.nl
  Curses.noecho
  Curses.curs_set 0

  colonize_tables(tables)

  place_string(0, 1, "Dumping #{tables.size} tables in DB #{options[:db]} to #{options[:dir]}")
  tables.each do |tbl|
    table_status(tbl, '')
  end
  Curses.refresh

  Parallel.map(tables, in_threads: options[:threads]) do |table|
    table_status(table, '...')
    Curses.refresh

    file_schema = "#{options[:dir]}/#{table}_schema.sql"
    file_constraints = "#{options[:dir]}/#{table}_constraints.sql"
    file_data = "#{options[:dir]}/#{table}_data.sql"
    file_checksum = "#{options[:dir]}/#{table}_checksum.dat"
    schema = `#{mysqldump_e} #{args} --no-data --tables #{table} | grep -v '.SQL_LOG_BIN' | grep -v '.GTID_PURGED' 2>/dev/null`
    constraints = schema.scan /(,[^A-Z\)]*(CONSTRAINT[^\n,]*))/
    if constraints.size > 0
      constraints.each do |el|
        schema.gsub! el[0], ''
      end
      constraints = "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n"\
        "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n"\
        "ALTER TABLE #{table} ADD " + constraints.map {|el| el[1] }.join(",\n ADD ") + ";\n"\
        "/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n"\
        "/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;"

      File.open(file_constraints, 'wb') do |f|
        f.write(constraints)
      end
    end
    File.open(file_schema, 'wb') do |f|
      f.write(schema)
    end
    `#{mysqldump_e} #{args} --no-create-info --tables #{table} | grep -v '.SQL_LOG_BIN' | grep -v '.GTID_PURGED' > #{file_data} 2>/dev/null`

    schema = `#{mysql_e} #{args} -e 'show create table #{table};' 2>/dev/null`
    count = `#{mysql_e} #{args} -e 'select count(*) from #{table};' 2>/dev/null`
    File.open(file_checksum, 'wb') do |f|
      f.write(Marshal.dump({ hash: Digest::MD5.hexdigest(schema), count: count }))
    end

    log(" - #{table} dump completed.")
    table_status(table, 'OK')
    Curses.refresh
  end

  place_string(0, 1, "Dump finished, press any key".ljust(Curses.cols - 1))
  Curses.refresh
  Curses.getch
elsif options[:action] == 'load'
  tables = Dir["#{options[:dir]}/*_schema.sql"].map do |file|
    match = file.match /\/(.*)_schema.sql/
    match[1]
  end

  if tables.size == 0
    puts "No tables to dump"
    exit
  end

  tables.sort!

  db = `#{mysql_e} #{args} -e 'show databases;' | grep '#{options[:db]}'`
  if db.empty?
    puts "Database #{options[:db]} doesn't exist, create it before load"
    exit
  end

  log("Starting load from #{options[:dir]} to database #{options[:db]}. Total #{tables.size} to load.")

  Curses.init_screen
  Curses.nl
  Curses.noecho
  Curses.curs_set 0

  colonize_tables(tables)
  %w(schema data constraints).each_with_index do |type, i|
    log("Loading #{type} to database #{options[:db]}.")
    place_string(0, 1, "[#{i + 1}/4] Loading #{type} for #{tables.size} tables to DB #{options[:db]} from #{options[:dir]}")

    tables.each do |tbl|
      table_status(tbl, '')
    end
    Curses.refresh

    Parallel.map(tables, in_threads: options[:threads]) do |table|
      table_status(table, '...')
      Curses.refresh

      file = "#{options[:dir]}/#{table}_#{type}.sql"
      if File.exist?(file)
        `#{mysql_e} #{args} -e 'source #{file};' 2>>#{__dir__}/sql_dumper.log`

        table_status(table, 'OK')
        log(" - #{table} #{type} load complete")
      else
        table_status(table, '--')
        log(" - #{table} #{type} file doesn't exist")
      end
      Curses.refresh
    end

    place_string(0, 1, "Loading #{type} finished".ljust(Curses.cols - 1))
    Curses.refresh
    sleep(5)
    Curses.clear
  end

  log("Starting checksum checks in #{options[:db]}.")
  place_string(0, 1, "[4/4] Starting checksum checks in #{options[:db]}")
  tables.each do |tbl|
    table_status(tbl, '')
  end
  Curses.refresh
  Parallel.map(tables, in_threads: options[:threads]) do |table|
    table_status(table, '...')
    Curses.refresh

    file = "#{options[:dir]}/#{table}_checksum.dat"
    schema = `#{mysql_e} #{args} -e 'show create table #{table};' 2>/dev/null`
    count = `#{mysql_e} #{args} -e 'select count(*) from #{table};' 2>/dev/null`
    checksum = Marshal.load(File.read(file))

    if checksum[:hash] == Digest::MD5.hexdigest(schema) && checksum[:count] == count
      table_status(table, 'OK')
      log(" - #{table} checksum verified")
    else
      status = ''
      unless checksum[:hash] == Digest::MD5.hexdigest(schema)
        status << 'M'
        log(" - #{table} schema doesn't match")
      end
      unless checksum[:count] == count
        status << 'C'
        log(" - #{table} data count doesn't match")
      end
      table_status(table, status)
    end
    Curses.refresh
  end

  place_string(0, 1, "Load finished, press any key".ljust(Curses.cols - 1))
  Curses.refresh
  Curses.getch
end
