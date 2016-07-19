#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'FileUtils'

class S3BucketCleaner
  attr_reader :bucket_name, :temp_files_list, :output_file

  def self.clean(bucket_name)
    new(bucket_name).clean
  end

  def initialize(bucket_name)
    @bucket_name     = bucket_name
    @temp_files_list = 'files_list.csv'
    @output_file     = 'aws_commands.sh'
  end

  def clean
    handle_undeleted_files
    handle_deleted_files
  end

  def handle_undeleted_files
    handle_files{ `#{undeleted_command}` }
  end

  def undeleted_command
    "echo '#!/bin/bash' > #{temp_files_list} && aws --output text s3api list-object-versions --bucket #{bucket_name} | grep -E \"^VERSIONS\" | awk '{print \"\"$4\",\"$8\"\"}' > #{temp_files_list}"
  end

  def handle_deleted_files
    handle_files{ `#{deleted_command}` }
  end

  def handle_files
    File.open(temp_files_list, 'w') {}
    yield
    create_batch_delete
    `/bin/bash #{output_file}`
    remove_files
  end

  def remove_files
    FileUtils.rm(temp_files_list)
    FileUtils.rm(output_file)
  end

  def deleted_command
    "echo '#!/bin/bash' > #{temp_files_list} && aws --output text s3api list-object-versions --bucket #{bucket_name} | grep -E \"^DELETEMARKERS\" | awk '{print \"\"$3\",\"$5\"\"}' >> #{temp_files_list}"
  end

  def create_batch_delete
    File.open(output_file, 'w') {|f| f.puts '#!/bin/bash'}
    lines              = []
    nb_lines_processed = 0
    IO.foreach(temp_files_list) do |line|
      nb_lines_processed += 1
      lines << line
      if lines.size >= 1000
        add_to_batch_delete(lines)
        lines = []
      end
    end
    add_to_batch_delete(lines)
    nb_lines_processed
  end

  def add_to_batch_delete(lines)
    lines = ::CSV.parse(lines.join, headers: false) rescue nil
    if lines && lines.any?
      objects   = lines.select{|line| line[0] && line[1]}.map{ |line| { "Key" => line[0],'VersionId' => line[1] } }
      structure = { 'Objects' => objects, 'Quiet' => true }
      puts structure.to_json
      command   = "aws s3api delete-objects --bucket #{bucket_name} --delete '#{structure.to_json}'"
      open(output_file, 'a') do |f|
        f.puts command
      end
    end
  end
end

bucket_name = ARGV[0]
S3BucketCleaner.clean(bucket_name)