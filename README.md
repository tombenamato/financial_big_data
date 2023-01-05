For our project we will use tick data for multiple assets (buy & sell price, ask & bid Volume) from dukascopy https://www.dukascopy.com/swiss/english/marketwatch/historical/.
From that, we are going to estimate the covariance matrix and clean the noise as we learned in class. In particular, we want to use the average oracle covariance cleaning technique, as our interest is for high frequency trading portfolio (and average oracle is wonderful on time complexity and hence it could be used in high frequency settings).

We thought about estimating, from a rolling window, the covariance matrix at every small-time step, and then rebalance our portfolio such that it is mean-variance optimal for each small-time step. The problem is that we do not have mean estimates, but we thought we could make some assumptions on it such as a constant return or a constant sharp ratio. We could also make the previously mentioned assumptions but from larger time-steps (i.e. estimate the covariance matrix from 1 hour data and from that obtain an expected return that we multiply by a coefficient to obtain an expected return for the small-time step).

For a first step we are going to assume mean return are 0, hence we do a min-variance optimization.

We would very much appreciate any suggestion on things we could add or modify from this idea. Needless to say that, if you have further questions or something is not too clear, do no hesitate to email us. We would also be totally willing to organize a meeting.
