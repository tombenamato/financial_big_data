import numpy as np
import pandas as pd
import fastcluster
from statsmodels.stats.correlation_tools import cov_nearest

def dist(R):
    N = R.shape[0]
    d = R[idx]
    out = fastcluster.average(d)
    rho = 1 - out[:, 2]
    #
    # Genealogy Set
    dend = [([i], []) for i in range(N)] + [[] for _ in range(out.shape[0])]

    for i, (a, b) in enumerate(out[:, :2].astype(int)):
        dend[i + N] = dend[a][0] + dend[a][1], dend[b][0] + dend[b][1]

    return dend[N:], rho


def AvLinkC(Dend, rho, R):
    N = R.shape[0]
    Rs = np.zeros((N, N))

    for (a, b), r in zip(Dend, rho):
        z = np.array(a).reshape(-1, 1), np.array(b).reshape(1, -1)
        Rs[z] = r

    Rs = Rs + Rs.T

    np.fill_diagonal(Rs, 1)
    return Rs


def noise(T, epsilon=1e-10):
    return np.random.normal(0, epsilon, size=(T))


def no_neg(x):
    l, v = np.linalg.eigh(x)
    p = l > 0
    l, v = l[p], v[:, p]
    return np.dot(v * l, v.T)


def near(x, niter=100, eigtol=1e-6, conv=1e-8):
    D = np.zeros(x.shape)
    diag = x.diagonal()

    for _ in range(niter):

        y = x
        R = x - D
        x = no_neg(R)
        D = x - R
        np.fill_diagonal(x, diag)

        if np.linalg.norm(x - y, ord=np.inf) / np.linalg.norm(y, ord=np.inf) < conv:
            break
    else:
        print("it didn't converge")
    return x


def HigherOrder(C, K):
    Cf = np.identity(C.shape[0])
    for i in range(max(K)):
        res = C - Cf
        dend, rho = dist(1 - res)
        res = AvLinkC(dend, rho, res)
        np.fill_diagonal(res, 0)
        Cf += res
        if i + 1 in K:
            yield Cf.copy()


def covariances_Hayashi_Yoshida(asset1: pd.DataFrame, asset2: pd.DataFrame, N_boot):
    asset1 = asset1.add_prefix("l_")
    asset2 = asset2.add_prefix("r_")
    logret12 = pd.DataFrame(asset1).join(pd.DataFrame(asset2), how="outer")
    logret12.ffill(inplace=True)
    logret12.dropna(inplace=True)
    logret12 = logret12.to_numpy()
    logret1 = logret12[:,:logret12.shape[1]//2]
    logret2 = logret12[:, logret12.shape[1]//2:]
    covs =(logret1 * logret2).sum(axis=0).reshape(1,-1)
    return covs


def filterCovariance(x, K=1, Nboot=100, method='near', is_correlation=False):
    '''
    Fiter covariance with k-BAHC
    input
    x (list[pd.DataFrame]): list of log return dataframe
    K = recursion order. K can be a list if you want to compute different order simultaneusly, K=1 is the standard BAHC.
    Nboot: Number of bootstraps
    method: regularization of negative eigenvalues. 'no-neg' set them to zeros, 'near' find the neareset semi-positive matrix
    is_correlation: Set to True if you want to filter the correlation

    output
    Fitered covariance matrix NxN. If K is a list, then is then the output is a list of matrices NxN
    '''
    x = x.copy()
    is_int = type(K) == int
    if is_int == True:
        K = [K]

    f = {'no-neg': no_neg, 'near': cov_nearest}

    N = len(x)
    global idx
    idx = np.triu_indices(N, 1)
    C = np.zeros((len(K), N, N))

    # create noise for each log return
    for idx_asset in range(len(x)):
        log_ret = x[idx_asset]
        T = log_ret.shape[0]
        prices = log_ret.to_numpy().reshape(-1)
        prices_with_noises =  noise(T).reshape(1, -1).repeat(Nboot, axis =0)
        prices_with_noises[0] = np.zeros((T))
        rng = np.random.default_rng()
        for i in range(Nboot):
            rng.shuffle(prices_with_noises[i])
            prices_with_noises[i] += prices
        x[idx_asset] = pd.DataFrame(index = log_ret.index, data= prices_with_noises.T)
    # calculate the covariance boost

    cov_boosts = np.zeros((Nboot, len(x), len(x)))
    for i, asset1 in enumerate(x):
        for j, asset2 in enumerate(x[i:], start=i):
            cov_boosts[:, i, j] = covariances_Hayashi_Yoshida(asset1, asset2, Nboot)
            cov_boosts[:, j, i] = cov_boosts[:, i, j]
    for cov in cov_boosts:
        standard_deviations = np.sqrt(np.diag(cov))
        si_sj = np.outer(standard_deviations, standard_deviations)
        Cb = cov / si_sj
        C += np.array(list(map(f[method], (np.array(list(HigherOrder(Cb, K)))))))

    if is_correlation == False:
        # std without noises, first boost is w.o noises
        standard_deviations = np.sqrt(np.diag(cov_boosts[0]))
        si_sj = np.outer(standard_deviations, standard_deviations)
        C = (C / Nboot) * si_sj
    else:
        C = (C / Nboot)

    if is_int == True:
        C = C[0]

    return C
