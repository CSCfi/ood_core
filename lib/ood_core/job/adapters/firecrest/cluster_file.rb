class OodCore::Job::Adapters::FirecREST::ClusterFile

  attr_reader :path, :adapter

  delegate :basename, :descend, :parent, :join, :to_s, to: :path

  def initialize(path, adapter)
    @adapter = adapter
    @path = path
  end

  def remote_type
    "cluster"
  end

  def raise_if_cant_access_directory_contents; end

  def directory?
    info = adapter.file_info(path)
    info.include?("directory") && !info.include?("symbolic link")
  end

  def ls
    files = adapter.list_files(path, show_hidden_files: true)
    files
      .select { |file| file["type"] == "-" || file["type"] == "d" } # Ignore symlinks and other special files
      .map do |file|
        {
          id:         path.join(file["name"]),
          name:       file["name"],
          size:       file['size'],
          human_size: human_size(file),
          directory:  file["type"] == "d",
          date:       DateTime.parse(file['last_modified']).to_time.to_i,
          owner:      file["user"],
          mode:       '', # TODO: parse the mode
          dev:        0
        }
    rescue => e
      # Ignore file if parsing errors occur
      nil
    end.compact.sort_by { |p| p[:directory] ? 0 : 1 }
  end

  def human_size(file)
    ::ApplicationController.helpers.number_to_human_size(file["size"], precision: 3)
  end

  def can_download_as_zip?(*)
    [false, 'Downloading remote files as zip is currently not supported']
  end

  def editable?
    false
  end

  def read(&block)
    adapter.download(path, &block)
  end

  def touch
  end

  def mkdir
  end

  def write(content)
  end

  def handle_upload(tempfile)
  end

  def mime_type
    ""
  end

  def send_small_file(response, download:, type:)
    # Need to attempt to read before setting headers in case it is not a small file.
    data = read
    response.set_header('X-Accel-Buffering', 'no')
    response.sending_file = true
    response.set_header("Last-Modified", Time.now.httpdate)
    if download
      response.set_header("Content-Type", type) if type.present?
      response.set_header("Content-Disposition", "attachment")
    else
      response.set_header("Content-Type", Files.mime_type_for_preview(type)) if type.present?
      response.set_header("Content-Disposition", "inline")
    end
    begin
      response.stream.write(data)
      # Need to rescue exception when user cancels download.
    rescue ActionController::Live::ClientDisconnected => e
    ensure
      response.stream.close
    end
  end

  def send_large_file(controller, download:, type:)
    # Redirect user directly to the URL where file can be downloaded.
    external_url = adapter.xfer_download(path)
    controller.redirect_to external_url, :status => :see_other
  end

  def send_file(controller, download:, type:)
    # Assume file is small, fall back to the large file handling if it is not.
    begin
      send_small_file(controller.response, download: download, type: type)
    rescue StandardError => e
      send_large_file(controller, download: download, type: type)
    end
  end

  def to_str
    to_s
  end
end
