# frozen_string_literal: true

module OodCore
  module Files
    class Adapter
      def initialize(*); end

      def directory?(path)
        raise NotImplementedError, 'files adapter did not define #directory?'
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
    end
  end
end
