from ..batches import load_results, STATIC_MINIMA
from ..cost import cost_ci, compare_results



if __name__ == '__main__':
    print map(cost_ci, load_results('jobs/auto')[1:5])
