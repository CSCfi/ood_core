# frozen_string_literal: true

module OodCore
  module Files
    class Adapter
      def initialize(*); end

      def dataroot
        raise NotImplementedError, 'files adapter did not define #dataroot'
      end

      def stat(path)
        raise NotImplementedError, 'files adapter did not define #stat'
      end

      def directory?(path)
        raise NotImplementedError, 'files adapter did not define #directory?'
      end

      def exist?(path)
        raise NotImplementedError, 'files adapter did not define #stat'
      end

      def ls(path)
        raise NotImplementedError, 'files adapter did not define #ls'
      end

      def editable?(path)
        raise NotImplementedError, 'files adapter did not define #editable?'
      end

      def read(path)
        raise NotImplementedError, 'files adapter did not define #read'
      end

      def touch(path)
        raise NotImplementedError, 'files adapter did not define #touch'
      end

      def mkdir(path)
        raise NotImplementedError, 'files adapter did not define #mkdir'
      end

      def write(path, content)
        raise NotImplementedError, 'files adapter did not define #write'
      end

      def size(path)
        raise NotImplementedError, 'files adapter did not define #size'
      end

      def mime_type(path)
        raise NotImplementedError, 'files adapter did not define #mime_type'
      end

      def move_with_progress(src_fs, dest_fs, src, dst, &block)
        raise NotImplementedError, 'files adapter did not define #mime_type'
      end

      def copy_with_progress(src_fs, dest_fs, src, dst, &block)
        raise NotImplementedError, 'files adapter did not define #mime_type'
      end

      def remove_with_progress(path, &block)
        raise NotImplementedError, 'files adapter did not define #mime_type'
      end
    end
  end
end
