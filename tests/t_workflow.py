# Import the workflow library.

# import ska_sdp_workflow as workflow
import sys
sys.path.append('/Users/pvw24040/SDP-SKA-SIM/SIM-566_implement_workflow_lib/sdp-workflow/src/ska_sdp_workflow/')

# print(sys.path)
from workflow import Workflow

print(Workflow())

# # === Workflow control and monitoring ===
#
# # We probably won't need an explicit initialisation function like this to
# # return a workflow instance, since it should be unique.
#
# wf = workflow.start()  # <-- Not needed!
#
# # Get workflow definition (type, id and version) from processing block.
#
# definition = workflow.get_definition()
#
# # Get workflow parameters from processing block as a dict, parsing with schema
# # if supplied
#
# parameters = workflow.get_parameters(schema=schema)
#
# # Make resource request, assuming input is in the form of a dict. Resource
# # object is used to deploy EEs, see below. Blocks until resources are
# # available?
#
# request = {'...': '...'}
# resources = workflow.resource_request(request)
#
# # The resources object may need methods to split it into subsets if we need to
# # deploy multiple EEs. Assume it returns a tuple/list containing the subsets.
#
# resources_subsets = resources.split('...')
#
# # This needs some smart handling to make sure resources can not be
# # oversubscribed if different splits are defined using the same underlying
# # resources.
#
# # Release the resources using a method on the object.
#
# resources.release()
#
# # For real-time workflows, wait for something to change in the SBI. Blocking
# # function. Not sure if this is the right idea....
#
# workflow.wait_sbi('...')
#
# # Signal workflow exit. Might only be in needed in the case of an abnormal
# # exit. We should be able to register an atexit handler to handle normal exit.
#
# workflow.exit(status='...')
#
# # === Execution Engines ===
#
# # EEs are deployed by using the resource object, using a function of the form
# # below which returns an EE object.
#
# ee = resources.ee_deploy_ < eetype > ()
#
# # The EE object will have a method to remove the deployment.
#
# ee.remove()
#
# # --- Dask EE ---
#
# # Deploy Dask with specified number of workers.
#
# ee_dask = resources.ee_deploy_dask(nworkers=4)
#
# # Dask needs a method to execute a function. Should handle communication with
# # the scheduler. Assume we pass the parameters as a dictionary.
#
# ee_dask.execute(function, params={'...': '...'})
#
# # This method will need to be non-blocking if functions are to be dispatched
# # simultaneously on multiple EEs. Needs to return a handle and wait on that if
# # needed.
#
# # --- Serial EE ---
#
# # By analogy with Dask, we should be able to create an EE to execute functions
# # on a single process. Like Dask, use cloudpickle to send the function to the
# # process.
#
# ee_serial = resources.ee_deploy_serial()
#
# # Use the same interface as the Dask EE to execute the function.
#
# ee_serial.execute(function, params={'...': '...'})
#
# # --- Helm EE ---
#
# # Some EE deployments, like the visibility receive, will use custom Helm charts
# # at least for the time being. Need to be able to deploy them like they are
# # done now.
#
# ee_helm = resources.ee_deploy_helm(chart='...', values={'...': '...'})
#
# # This won't have an execute method since it will be a one-shot execution. It
# # might need a method to block until the execution to complete if it is used in
# # batch processing.
#
# ee_helm.wait()