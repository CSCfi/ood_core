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
    true
  end

  def ls
    []
  end

  def human_size
    '-'
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
