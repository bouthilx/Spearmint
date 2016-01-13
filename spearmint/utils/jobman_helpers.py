import socket
import sys
import time
import traceback

from jobman.channel import Channel

channel = Channel()

status_converter = {
    channel.COMPLETE: 'completed',
    channel.INCOMPLETE: 'pending',

    # Not fully supported
    channel.ERR_START: 'broken',
    channel.ERR_SYNC: 'broken',
    channel.ERR_RUN: 'broken',
    channel.CANCELED: 'broken',

    channel.START: 'pending',
    channel.RUNNING: 'pending',
    channel.DONE: 'completed'
}


def convert_state_output_to_spearmint(jobman_function):

    def call(state, channel):
        state["ran_on_node"] = socket.gethostname()
        start_time = time.time()
        state["start_time"] = start_time

        try:
            status = jobman_function(state, channel)
        except BaseException:
            status = Channel.ERR_RUN
            traceback.print_exc()
            sys.stderr.write("Problem executing the function\n")
            return channel.ERR_RUN

        sys.stdout.flush()
        sys.stderr.flush()

        end_time = time.time()

        result = dict([(task, state[task])
                       for task in state["tasks"] if task in state])

        if set(result.keys()) != set(state['tasks']):
            raise Exception("There are missing tasks in output state of "
                            "function %s:  %s" %
                            (jobman_function.__name__,
                             str(set(state['tasks']) - set(result.keys()))))

        state["values"] = result

        sys.stdout.flush()
        sys.stderr.flush()

        state["proc_status"] = status_converter[status]
        state["end_time"] = end_time

        return status

    return call
