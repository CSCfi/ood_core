# frozen_string_literal: true

require 'ood_core/job/adapters/helper'
require 'open3'
require 'date'

module OodCore
  module Files
    class Factory
      def self.build_ssh(config)
        c = config.to_h.symbolize_keys
        host = c.fetch(:host, nil)
        bin_overrides = c.fetch(:bin_overrides, {})
        Adapters::Ssh.new(host: host, bin_overrides: bin_overrides)
      end
    end

    module Adapters
      # An adapter that provides access to files over SSH.
      class Ssh < Adapter
        UNESCAPES = {
          'a' => "\x07", 'b' => "\x08", 't' => "\x09",
          'n' => "\x0a", 'v' => "\x0b", 'f' => "\x0c",
          'r' => "\x0d", 'e' => "\x1b", '\\\\' => "\x5c",
          '"' => "\x22", "'" => "\x27"
        }.freeze

        LS_REGEX = Regexp.new('^(?<type>[ld-])(?<perms>\S{9})(?<acl>\S?)\s+(?<nlinks>\d+)\s+(?<user>\S+)\s+(?<group>\S+)\s+(?<size>\d+)\s+(?<time>\S+)\s+"(?<name>.+)"$')
        PROGRESS_REGEX = /\s*\d+\s+(?<progress>\d+)%/.freeze

        attr_reader :host, :bin_overrides

        def initialize(host: nil, bin_overrides: {})
          @host = host
          @bin_overrides = bin_overrides
          super
        end

        class << self
        end
        # Helper fuction for unescaping file names from ls that were escaped with --quoting-style=c
        # Modified from https://stackoverflow.com/questions/8639642/best-way-to-escape-and-unescape-strings-in-ruby
        def unescape(str)
          str.b.gsub(/\\(?:([#{UNESCAPES.keys.join}])|(\d{3}))/) do
            if Regexp.last_match(1)
              Regexp.last_match(1) == '\\' ? '\\' : UNESCAPES[Regexp.last_match(1)]
            elsif Regexp.last_match(2) # escape \000
              [Regexp.last_match(2).to_i(8)].pack('c')
            end
          end
        end

        def call(cmd, *args, env: {}, stdin: '')
          args = args.map(&:to_s)
          cmd, args = OodCore::Job::Adapters::Helper.ssh_wrap(host, cmd, args, false, {}, nil, bin_overrides)
          Open3.capture3(env, cmd, *args.map(&:to_s), stdin_data: stdin.to_s)
        end

        def rsync(*args, env: {}, stdin: '')
          args = args.map(&:to_s)
          ssh_cmd, ssh_args = OodCore::Job::Adapters::Helper.ssh_wrap(host, nil, [], false, {}, nil, bin_overrides)
          rsync = OodCore::Job::Adapters::Helper.bin_path('rsync', '', bin_overrides)
          ssh_args = ssh_args[0..-3]
          args = ['-e', "#{Shellwords.escape(ssh_cmd)} #{ssh_args.join(' ')}"] + args
          Open3.capture3(env, rsync, *args.map(&:to_s), stdin_data: stdin.to_s)
        end

        def rsync_popen(*args, stdin_data: nil, &block)
          args = args.map(&:to_s)
          ssh_cmd, ssh_args = OodCore::Job::Adapters::Helper.ssh_wrap(host, nil, [], false, {}, nil, bin_overrides)
          rsync = OodCore::Job::Adapters::Helper.bin_path('rsync', '', bin_overrides)
          ssh_args = ssh_args[0..-3]
          args = ['-e', "#{Shellwords.escape(ssh_cmd)} #{ssh_args.join(' ')}"] + args

          Open3.popen3(rsync, *args.map(&:to_s)) do |i, o, e, t|
            i.write(stdin_data) if stdin_data
            i.close

            err_reader = Thread.new { e.read }

            yield o

            o.close
            exit_status = t.value
            err = err_reader.value.to_s.strip
            if err.present? || !exit_status.success?
              raise StandardError.new(exit_status.exitstatus), "rsync exited with status #{exit_status.exitstatus}\n#{err}"
            end
          end
        end

        def rsync_with_progress(src, dst, src_fs: nil, dest_fs: nil, move: false)
          full_src = src
          full_dst = dst
          full_src = "#{src_fs}:#{src}" if src_fs
          full_dst = "#{dest_fs}:#{dst}" if dest_fs
          dir = src_fs ? directory?(src) : File.directory?(src)
          full_src = "#{full_src}/" if dir
          rsync_popen(*['-a', '--partial', '--info=all0,progress2', move ? '--remove-source-files' : nil, full_src, full_dst].compact) do |o|
            o.each_line("\r") do |line|
              match = line.match(PROGRESS_REGEX)
              next unless match

              progress = match[:progress].to_i
              yield progress
            end
          end
        end

        def call_popen(cmd, *args, env: {}, stdin: nil)
          args = args.map(&:to_s)
          cmd, args = OodCore::Job::Adapters::Helper.ssh_wrap(host, cmd, args, false, {}, nil, bin_overrides)
          Open3.popen3(env, cmd, *args.map(&:to_s)) do |i, o, e, t|
            i.write(stdin) if stdin
            i.close

            err_reader = Thread.new { e.read }

            yield o

            o.close
            exit_status = t.value
            err = err_reader.value.to_s.strip
            if err.present? || !exit_status.success?
              raise StandardError.new(exit_status.exitstatus),
                    "Command exited with status #{exit_status.exitstatus}\n#{err}"
            end
          end
        end

        def directory?(path)
          stdout, stderr, status = call('stat', '--dereference', '--format', '%F', Shellwords.escape(path))
          if status.success?
            stdout.strip == 'directory'
          elsif status.exitstatus == 1
            raise StandardError, "Path does not exist: #{path}"
          else
            raise StandardError, "Could not stat #{path}: #{stderr}"
          end
        end

        # Parses permissions in rwxrwxrwx format into octal mode.
        # Ignores sticky, setuid and setgid bits.
        def parse_perms(perms)
          return 0 unless perms.length == 9

          perms.chars.each_slice(3).to_a.map do |r, w, x|
            r_ = r == 'r' ? 4 : 0
            w_ = w == 'w' ? 2 : 0
            x_ = ['x', 's', 't'].include?(x) ? 1 : 0
            r_ + w_ + x_
          end.map(&:to_s).join.to_i(8)
        end

        def ls(path)
          # Due to lack of better alternatives, parse the output of ls.
          # Follow symlinks to show info about target in the file browser instead of symlink.
          args = ['-l', '--almost-all', '--quoting-style=c', '--time-style=+%Y-%m-%dT%H:%M:%S%z', '--dereference',
                  Shellwords.escape(path)]

          stdout, stderr, status = call('ls', *args)
          Rails.logger.warn("ls exited with non-zero status #{status.exitstatus}: #{stderr}") unless status.success?
          stdout.lines.drop(1).map do |line|
            match = line.match(LS_REGEX)
            # Ignore sockets, named pipes and other special files.
            next if match.nil?

            escaped_name, size, type, time, user, perms = match.named_captures.values_at('name', 'size', 'type', 'time',
                                                                                         'user', 'perms')
            name = unescape(escaped_name).force_encoding('utf-8')
            {
              id:           File.join(path, name),
              name:         name,
              size:         type == 'd' ? nil : size.to_i,
              directory:    type == 'd',
              date:         DateTime.parse(time).to_time.to_i,
              owner:        user,
              mode:         parse_perms(perms),
              dev:          0,
              downloadable: true # TODO: handle cases where it is not downloadable
            }
          end.compact
        end

        def editable?(path)
          _, _, status = call('test', '-f', Shellwords.escape(path), '-a', '-r', Shellwords.escape(path), '-a', '-w',
                              Shellwords.escape(path))
          status.success?
        end

        def read(path, &_block)
          if block_given?
            call_popen('cat', Shellwords.escape(path)) do |o|
              while (data = o.read(32_768))
                yield data
              end
            end
          else
            stdout, stderr, status = call('cat', Shellwords.escape(path))
            unless status.success?
              err = stdout.blank? ? stderr : stdout
              raise StandardError, "Could not read file #{path}: #{err}"
            end
            stdout
          end
        end

        def touch(path)
          stdout, stderr, status = call('touch', Shellwords.escape(path))
          return if status.success?

          err = stdout.blank? ? stderr : stdout
          raise StandardError, "Could not touch #{path}: #{err}"
        end

        def mkdir(path)
          stdout, stderr, status = call('mkdir', Shellwords.escape(path))
          return if status.success?

          err = stdout.blank? ? stderr : stdout
          raise StandardError, "Could not create directory #{path}: #{err}"
        end

        def write(path, content)
          stdout, stderr, status = call('cat', ">#{Shellwords.escape(path)}", stdin: content)
          return if status.success?

          err = stdout.blank? ? stderr : stdout
          raise StandardError, "Could not write #{path}: #{err}"
        end

        def size(path)
          stdout, stderr, status = call('stat', '--dereference', '--format', '%s', Shellwords.escape(path))
          if status.success?
            stdout.to_i
          elsif status.exitstatus == 1
            raise StandardError, "Path does not exist: #{path}"
          else
            raise StandardError, "Could not stat #{path}: #{stderr}"
          end
        end

        def mime_type(path)
          stdout, stderr, status = call('file', '--dereference', '-Eb', '--mime-type', Shellwords.escape(path))
          unless status.success?
            err = stdout.blank? ? stderr : stdout
            raise StandardError, "Could not check file type of #{path}: #{err}"
          end

          type = stdout.strip

          # if you touch a file and it is empty, the mime type is "inode/x-empty"
          # but in our interaction with the file we would treat this as "text/plain"
          # so we return "text/plain" so web browsers treat it as so
          if type == 'inode/x-empty'
            'text/plain'
          else
            type
          end
        end

        def mv(src, dst)
          stdout, stderr, status = call('mv', Shellwords.escape(src), Shellwords.escape(dst))
          return if status.success?

          err = stdout.blank? ? stderr : stdout
          raise StandardError, "Could not move #{src} to #{dst}: #{err}"
        end

        def cp(src, dst)
          stdout, stderr, status = call('cp', '-r', Shellwords.escape(src), Shellwords.escape(dst))
          return if status.success?

          err = stdout.blank? ? stderr : stdout
          raise StandardError, "Could not copy #{src} to #{dst}: #{err}"
        end

        def remove(path)
          stdout, stderr, status = call('rm', '-r', Shellwords.escape(path))
          return if status.success?

          err = stdout.blank? ? stderr : stdout
          raise StandardError, "Could not remove #{path}: #{err}"
        end

        def move_with_progress(src_fs, dest_fs, src, dst, &block)
          # use rsync for local<->cluster and mv for cluster<->cluster
          from_cluster = src_fs.respond_to?(:file_adapter) && src_fs.file_adapter.instance_of?(self.class)
          to_cluster = dest_fs.respond_to?(:file_adapter) && dest_fs.file_adapter.instance_of?(self.class)
          within_cluster = from_cluster && to_cluster
          if within_cluster
            mv(src, dst, &block)
          else
            rsync_with_progress(src, dst, src_fs: from_cluster && host, dest_fs: to_cluster && host, move: true, &block)
          end
        end

        def copy_with_progress(src_fs, dest_fs, src, dst, &block)
          # use rsync for local<->cluster and cp for cluster<->cluster
          from_cluster = src_fs.respond_to?(:file_adapter) && src_fs.file_adapter.instance_of?(self.class)
          to_cluster = dest_fs.respond_to?(:file_adapter) && dest_fs.file_adapter.instance_of?(self.class)
          within_cluster = from_cluster && to_cluster
          if within_cluster
            cp(src, dst, &block)
          else
            rsync_with_progress(src, dst, src_fs: from_cluster && host, dest_fs: to_cluster && host, move: false, &block)
          end
        end

        def remove_with_progress(path, &block)
          rm(path)
        end
      end
    end
  end
end
