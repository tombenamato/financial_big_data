import numpy as np


def get_sortest_eig(C):
    '''
    input
        C: correlation matrix

    output:
        l: eigenvalues
        v: eigenvectors
    '''

    l, v = np.linalg.eigh(C)
    ordn = np.argsort(l)
    l, v = l[ordn], v[:, ordn]
    return l, v


class AO():
    def __init__(self, AO_values_path: str) -> None:
        """
        Initialise an average oracle object with the average oracles value situated in the csv file at AO_values_path.

        Args:
           AO_values_path (str): The path to the csv file where the average oracles values are
        """
        self.AO_values = np.genfromtxt(AO_values_path, delimiter=',')


    def filter_correlation_AO(self, C: np.ndarray) -> np.ndarray:
        """
        Filter the correlation matrix given and return it.
        Args:
           C (ndarray): The correlation to filter.
        Returns:
            ndarray : The filtered correlation matrix.
        """
        l, v = get_sortest_eig(C)
        C_AO = (v @ np.diag(self.AO_values) @ v.T)

        return C_AO

    def filter_covariance_AO(self, Sigma : np.ndarray) -> np.ndarray:
        """
        Filter the covariance matrix given and return it.
        Args:
           Sigma (ndarray): The covariance to filter.
        Returns:
            ndarray : The filtered covariance matrix.
        """
        s = np.sqrt(np.diag(Sigma))
        si_sj = np.outer(s, s)
        C = Sigma / si_sj
        C_AO = self.filter_correlation_AO(C)

        Sigma_AO = C_AO * si_sj

        return Sigma_AO
