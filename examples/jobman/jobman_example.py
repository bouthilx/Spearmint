import numpy as np
import math

from jobman import sql

from spearmint.utils.jobman import convert_state_output_to_spearmint


def branin(x, y):

    result = (np.square(y - (5.1 / (4 * np.square(math.pi))) * np.square(x) +
                        (5 / math.pi) * x - 6) +
              10 * (1 - (1. / (8 * math.pi))) * np.cos(x) + 10)

    result = float(result)

    print 'Result = %f' % result

    return result


@convert_state_output_to_spearmint
def jobman_main(state, channel):
    print 'Anything printed here will end up in the output directory of job #%d' % state[sql.JOBID]
    state[u'result'] = branin(np.array(state["params"]['x']["values"]),
                              np.array(state["params"]['y']["values"]))
    return channel.COMPLETE
