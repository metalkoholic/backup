# encoding: utf-8
require "google/api_client"
require "google_drive"

module Backup
  module Storage
    class Gdrive < Base
      include Storage::Cycler
      class Error < Backup::Error; end

      ##
      # Gdrive API credentials
      attr_accessor :client_id, :client_secret, :refresh_token, :folder_id

      ##

      def initialize(model, storage_id = nil)
        super

        @path ||= 'backups'

        check_configuration

        client = ::Google::APIClient.new
        auth = client.authorization
        auth.client_id = client_id
        auth.client_secret = client_secret
        auth.scope = "https://www.googleapis.com/auth/drive"
        auth.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
        auth.refresh_token = refresh_token
        auth.fetch_access_token!
        @session = ::GoogleDrive::Session.login_with_oauth(auth.access_token)
      end

      private
      def transfer!
        package.filenames.each do |filename|
          src = File.join(Config.tmp_path, filename)
          Logger.info "Storing '#{ filename }'..."
          file = @session.upload_from_file(src, filename, :convert => false)
          if folder_id.nil?
            folder = @session.collection_by_title(path).create_subcollection(package.time)
            @session.collection_by_title(path).subcollection_by_title(folder.title).add(file)
          else
            dir = @session.file_by_id(folder_id)
            folder = dir.create_subcollection(package.time)
            new_child = @session.drive.children.insert.request_schema.new('id' => file.id)
            @session.execute!(api_method:  @session.drive.children.insert, body_object: new_child, parameters:  {folderId: folder.id, childId: file.id})
          end
          
          @session.root_collection.remove(file)
        end
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{ package.time }..."
        if folder_id.nil?
          dir = @session.collection_by_title(path)
        else
          dir = @session.file_by_id(folder_id)
        end
        file = dir.subcollection_by_title(package.time)
        dir.remove(file)
      end

      def check_configuration
         required = %w{ client_id client_secret refresh_token }

        raise Error, <<-EOS if required.map {|name| send(name) }.any?(&:nil?)
          Configuration Error
          #{ required.map {|name| "##{ name }"}.join(', ') } are all required
        EOS
      end
    end
  end
end