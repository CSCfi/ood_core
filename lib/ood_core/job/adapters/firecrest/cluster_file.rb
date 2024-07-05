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
    yield ""
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

  def to_str
    to_s
  end
end
