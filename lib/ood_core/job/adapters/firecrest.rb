require "time"
require 'etc'
require "ood_core/refinements/hash_extensions"
require "ood_core/refinements/array_extensions"
require "ood_core/job/adapters/helper"

module OodCore
  module Job
    class Factory
      using Refinements::HashExtensions

      # Build the FirecREST adapter from a configuration
      def self.build_firecrest(config)
        c = config.to_h.symbolize_keys
        machine              = c.fetch(:cluster, nil)
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

          def initialize(machine: nil, endpoint: nil)
            @machine              = machine && machine.to_s
            @endpoint             = endpoint && endpoint.to_s
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
          raise NotImplementedError, "cluster_info not implemented in firecrest adapter yet"
        end

        # Retrieve info for all jobs from the resource manager
        # @raise [JobAdapterError] if something goes wrong getting job info
        # @return [Array<Info>] information describing submitted jobs
        # @see Adapter#info_all
        def info_all(attrs: nil)
          raise NotImplementedError, "info_all not implemented in firecrest adapter yet"
        end

        # Retrieve job info from the resource manager
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong getting job info
        # @return [Info] information describing submitted job
        # @see Adapter#info
        def info(id)
          raise NotImplementedError, "info not implemented in firecrest adapter yet"
        end

        # Retrieve job status from resource manager
        # @param id [#to_s] the id of the job
        # @raise [JobAdapterError] if something goes wrong getting job status
        # @return [Status] status of job
        # @see Adapter#status
        def status(id)
          raise NotImplementedError, "status not implemented in firecrest adapter yet"
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
          raise NotImplementedError, "delete not implemented in firecrest adapter yet"
        end

      end
    end
  end
end
