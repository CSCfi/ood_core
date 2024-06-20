require 'etc'
require 'json'
require 'jwt'
require 'net/http'
require 'net/http/post/multipart'
require "ood_core/refinements/hash_extensions"
require "ood_core/refinements/array_extensions"
require "ood_core/job/adapters/helper"
require 'time'
require 'uri'

module OodCore
  module Job
    class Factory
      using Refinements::HashExtensions

      # Build the FirecREST adapter from a configuration
      def self.build_firecrest(config)
        c = config.to_h.symbolize_keys
        machine              = c.fetch(:machine, nil)
        endpoint             = c.fetch(:endpoint, nil)
        firecrest = Adapters::FirecREST::Batch.new(machine: machine, endpoint: endpoint)
        Adapters::FirecREST.new(firecrest: firecrest)
      end
    end

    module Adapters
      # An adapter object that describes the communication with a Slurm
      # resource manager for job management.
      class FirecREST < Adapter
        using Refinements::HashExtensions
        using Refinements::ArrayExtensions

        # @api private
        class Batch

          attr_reader :machine
          attr_reader :endpoint

          class Error < StandardError; end
          class TokenError < Error; end
          class HttpError < Error; end
          class TaskError < Error; end

          def initialize(machine: nil, endpoint: nil)
            @machine              = machine && machine.to_s
            @firecrest_uri        = endpoint && endpoint.to_s
            @client_id            = ENV['FIRECREST_CLIENT_ID']
            @client_secret        = ENV['FIRECREST_CLIENT_SECRET']
            @token_uri            = ENV['FIRECREST_TOKEN_URI']
            @token                = nil
            @token_expiration     = nil
          end

          # Submit a job with a local script file to the batch server
          # @example
          #   my_batch.submit_job("/path/to/script.sh")
          # @raise FIXME
          # @return [void]
          def submit_job(script_path)
            response = http_post(
              "#{@firecrest_uri}/compute/jobs/upload",
              headers: build_headers,
              files: { "file" => script_path }
            )
            task_id = parse_response(response, "task_id")
            wait_task_result(task_id, 200)
          end

          # Delete a specified job from batch server
          # @example Delete job "1234"
          #   my_batch.delete_job("1234")
          # @param id [#to_s] the id of the job
          # @raise FIXME
          # @return [void]
          def delete_job(job_id)
            response = http_delete("#{@firecrest_uri}/compute/jobs/#{job_id}", headers: build_headers)
            task_id = parse_response(response, "task_id")
            res = wait_task_result(task_id, 200)
          end

          def get_active_jobs(job_ids: [])
            job_ids_str = job_ids.join(',')
            params = !job_ids.empty? ? { "jobs" => job_ids_str } : {}
            response = http_get("#{@firecrest_uri}/compute/jobs", headers: build_headers, params: params)
            task_id = parse_response(response, "task_id")
            res = wait_task_result(task_id, 200).values
          end

          def get_jobs(job_ids: [])
            job_ids_str = job_ids.join(',')
            params = !job_ids.empty? ? { "jobs" => job_ids_str } : {}
            response = http_get("#{@firecrest_uri}/compute/acct", headers: build_headers, params: params)
            task_id = parse_response(response, "task_id")
            res = wait_task_result(task_id, 200)
            # When there are no jobs, the response is {}, instead of empty list
            res.empty? ? [] : res
          end

          def get_nodes
            response = http_get("#{@firecrest_uri}/compute/nodes", headers: build_headers)
            task_id = parse_response(response, "task_id")
            res = wait_task_result(task_id, 200)
          end

          def get_groups
            response = http_get("#{@firecrest_uri}/utilities/whoami", headers: build_headers, params: { "groups" => true })
            parse_response(response, "output")["groups"]
          end

          STATE_MAP = {
            'BOOT_FAIL' => :completed,
            'CANCELLED' => :completed,
            'COMPLETED' => :completed,
            'DEADLINE' => :completed,
            'CONFIGURING' => :queued,
            'COMPLETING' => :running,
            'FAILED'  => :completed,
            'NODE_FAIL' => :completed,
            'PENDING' => :queued,
            'PREEMPTED' => :suspended,
            'REVOKEDRV' => :completed,
            'RUNNING'  => :running,
            'SPECIAL_EXIT' => :completed,
            'STOPPED' => :running,
            'SUSPENDED'  => :suspended,
            'TIMEOUT' => :completed,
            'OUT_OF_MEMORY' => :completed
          }

          def slurm_state_to_ood_state(state)
            STATE_MAP.each do |key, value|
              return value if state.include?(key)
            end
            :undetermined
          end

          def slurm_state_pending(state)
            pending_states = [
              'COMPLETING',
              'CONFIGURING',
              'PENDING',
              'RESV_DEL_HOLD',
              'REQUEUE_FED',
              'REQUEUE_HOLD',
              'REQUEUED',
              'RESIZING',
              'REVOKED',
              'SIGNALING',
              'SPECIAL_EXIT',
              'STAGE_OUT',
              'STOPPED',
              'SUSPENDED'
            ]

            if state
              state.split(',').any? { |s| pending_states.include?(s) }
            else
              false
            end
          end


          private

          def token_expired?
            return true unless @token_expiration
            Time.now >= @token_expiration
          end

          def decode_token_expiration(token)
            decoded_token = JWT.decode(token, nil, false)
            exp = decoded_token[0]['exp']
            @token_expiration = Time.at(exp)
          end

          def get_token
            return @token if @token && !token_expired?

            uri = URI(@token_uri)
            data = {
              grant_type: 'client_credentials',
              client_id: @client_id,
              client_secret: @client_secret
            }

            request = Net::HTTP::Post.new(uri)
            request['Content-Type'] = 'application/x-www-form-urlencoded'
            request.set_form_data(data)
            response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
              http.request(request)
            end
            raise TokenError, "Failed to obtain token: #{response.body}" if response.code.to_i != 200

            json_response = JSON.parse(response.body)
            @token = json_response["access_token"]
            decode_token_expiration(@token)
            @token
          rescue => e
            raise TokenError, "Token error: #{e.message}"
          end

          def build_headers
            { 'Authorization' => "Bearer #{get_token}", 'X-Machine-Name' => @machine }
          end

          def http_get(url, headers: {}, params: {})
            uri = URI(url)
            uri.query = URI.encode_www_form(params) unless params.empty?
            request = Net::HTTP::Get.new(uri)
            headers.each { |key, value| request[key] = value }

            response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
              http.request(request)
            end
            raise HttpError, "Error: #{response.code} #{response.body}" if response.code.to_i >= 400
            response
          end

          def http_delete(url, headers: {})
            uri = URI(url)
            request = Net::HTTP::Delete.new(uri)
            headers.each { |key, value| request[key] = value }

            response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
              http.request(request)
            end
            raise HttpError, "Error: #{response.code} #{response.body}" if response.code.to_i >= 400
            response
          end

          def http_post(url, headers:, data: {}, files: {})
            uri = URI.parse(url)

            # Prepare the form data, including files
            form_data = data
            files.each do |key, file_path|
              form_data[key] = Multipart::Post::UploadIO.new(File.open(file_path), 'application/octet-stream', File.basename(file_path))
            end

            request = Net::HTTP::Post::Multipart.new(uri.path, form_data)
            headers.each do |key, value|
              request[key] = value
            end

            http = Net::HTTP.new(uri.host, uri.port)
            http.use_ssl = (uri.scheme == 'https')
            resp = http.request(request)
            raise HttpError, "Error: #{resp.code} #{resp.body}" if resp.code.to_i >= 400
            resp
          end

          def parse_response(response, key)
            JSON.parse(response.body)[key]
          end

          def wait_task_result(task_id, final_status)
            task = get_task(task_id)
            while task["status"].to_i < final_status
              # TODO: change this to maybe exponential backoff (?)
              sleep 1
              task = get_task(task_id)
            end
            raise TaskError, "Error in task: #{task['data']}" if task["status"].to_i >= 400
            task["data"]
          end

          def get_task(task_id)
            response = http_get("#{@firecrest_uri}/tasks/#{task_id}", headers: build_headers)
            parse_response(response, "task")
          end
        end

        # @api private
        # @param opts [#to_h] the options defining this adapter
        # @option opts [Batch] :firecrest The FirecREST batch object
        # @see Factory.build_firecrest
        def initialize(opts = {})
          o = opts.to_h.symbolize_keys

          @firecrest = o.fetch(:firecrest) { raise ArgumentError, "No firecrest object specified. Missing argument: firecrest" }
        end

        # Submit a job with the attributes defined in the job template instance
        # @param script [Script] script object that describes the script and
        #   attributes for the submitted job
        # @param after [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution at any point after dependent jobs have started execution
        # @param afterok [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution only after dependent jobs have terminated with no errors
        # @param afternotok [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution only after dependent jobs have terminated with errors
        # @param afterany [#to_s, Array<#to_s>] this job may be scheduled for
        #   execution after dependent jobs have terminated
        # @raise [JobAdapterError] if something goes wrong submitting a job
        # @return [String] the job id returned after successfully submitting a
        #   job
        # @see Adapter#submit
        def submit(script, after: [], afterok: [], afternotok: [], afterany: [])
          raise NotImplementedError, "submit not implemented in firecrest adapter yet"
        end

        # Retrieve info about active and total cpus, gpus, and nodes
        # @return [Hash] information about cluster usage
        def cluster_info
          # TODO: We csn get some information from get_nodes
          node_info = @firecrest.get_nodes
          # TODO: We have the state of each node, so proably we can get this information
          number_of_active_nodes = nil
          ClusterInfo.new(active_nodes: number_of_active_nodes,
                          total_nodes: node_info.length,
                          active_processors: nil,
                          total_processors: nil,
                          active_gpus: nil,
                          total_gpus: nil
          )
        end

        # Retrieve info for all jobs from the resource manager
        # @raise [JobAdapterError] if something goes wrong getting job info
        # @return [Array<Info>] information describing submitted jobs
        # @see Adapter#info_all
        def info_all(attrs: nil)
          # Cannot get information for active jobs that don't belong to the user
          @firecrest.get_jobs.map do |v|
            parse_job_info(v)
          end
        end

        # Retrieve job info from the resource manager
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong getting job info
        # @return [Info] information describing submitted job
        # @see Adapter#info
        def info(id)
          id = id.to_s
          info_ary = @firecrest.get_jobs(job_ids: [id.to_s]).map do |v|
            parse_job_info(v)
          end

          # If no job was found we assume that it has completed
          info_ary.empty? ? Info.new(id: id, status: :completed) : info_ary.first
        end

        def parse_job_info(v)
          Info.new(
            id: v['job_id'],
            status: @firecrest.slurm_state_to_ood_state(v['state']),
            allocated_nodes: parse_nodes(v['nodelist']),
            submit_host: nil,
            job_name: v['name'],
            job_owner: v['user'],
            accounting_id: nil,
            procs: nil,
            queue_name: v['partition'],
            wallclock_time: duration_in_seconds(v['elapsed_time']),
            wallclock_limit: nil,
            cpu_time: nil,
            submission_time: nil,
            dispatch_time: v['start_time'] == "N/A" ? nil : Time.parse(v['start_time']),
            native: v,
            gpus: nil
          )
        end

        # Retrieve job status from resource manager
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong getting job status
        # @return [Status] status of job
        # @see Adapter#status
        def status(id)
          jobs = @firecrest.get_jobs([id.to_s])
          # TODO: need to check how firecrest deals with job arrays
          if job = jobs.detect { |job| job["job_id"] == id }
            Status.new(state: slurm_state_to_ood_state(job["state"]))
          else
            Status.new(state: :undetermined)
          end
        end

        # Put the submitted job on hold
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong holding a job
        # @return [void]
        # @see Adapter#hold
        def hold(id)
          raise NotImplementedError, "hold not implemented in firecrest adapter yet"
        end

        # Release the job that is on hold
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong releasing a job
        # @return [void]
        # @see Adapter#release
        def release(id)
          raise NotImplementedError, "release not implemented in firecrest adapter yet"
        end

        # Delete the submitted job
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong deleting a job
        # @return [void]
        # @see Adapter#delete
        def delete(id)
          @firecrest.delete_job(id)
        end

        # Convert host list string to individual nodes
        # "em082"
        # "em[014,055-056,161]"
        # "c457-[011-012]"
        # "c438-[062,104]"
        # "c427-032,c429-002"
        def parse_nodes(node_list)
          node_list.to_s.scan(/([^,\[]+)(?:\[([^\]]+)\])?/).map do |prefix, range|
            if range
              range.split(",").map do |x|
                x =~ /^(\d+)-(\d+)$/ ? ($1..$2).to_a : x
              end.flatten.map do |n|
                { name: prefix + n, procs: nil }
              end
            elsif prefix
              [ { name: prefix, procs: nil } ]
            else
              []
            end
          end.flatten
        end

        # Convert duration to seconds
        def duration_in_seconds(time)
          return 0 if time.nil?
          time, days = time.split("-").reverse
          days.to_i * 24 * 3600 +
            time.split(':').map { |v| v.to_i }.inject(0) { |total, v| total * 60 + v }
        end

      end
    end
  end
end
