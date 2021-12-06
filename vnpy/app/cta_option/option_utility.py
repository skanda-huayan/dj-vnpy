
import numpy as np
from scipy import stats

#################
# BSM模型相关
def get_option_d(s, k, t, r, sigma, q):
    d1 = (np.log(s/k) + (r - q + 0.5*sigma**2)*t)/(sigma*np.sqrt(t))
    d2 = (np.log(s/k) + (r - q - 0.5*sigma**2)*t)/(sigma*np.sqrt(t))
    return d1, d2

def get_option_greeks(cp, s, k, t, r, sigma, q):
    """
    计算期权希腊值
    :param cp:
    :param s:
    :param k:
    :param t:
    :param r:
    :param sigma:
    :param q:
    :return:
    """
    d1, d2 = get_option_d(s, k, t, r, sigma, q)
    delta = cp * stats.norm.cdf(cp * d1)
    gamma = stats.norm.pdf(d1) / (s * sigma * np.sqrt(t))
    vega = (s * stats.norm.pdf(d1) * np.sqrt(t))
    theta = (-1 * (s * stats.norm.pdf(d1) * sigma) / (2 * np.sqrt(t)) - cp * r * k * np.exp(-r * t) * stats.norm.cdf(cp * d2))
    return delta, gamma, vega, theta

def bsm_value(cp, s, k, t, r, sigma, q):
    d1, d2 = get_option_d(s, k, t, r, sigma, q)
    if cp > 0:
        value = (
            s*np.exp(-q*t)*stats.norm.cdf(d1) -
            k*np.exp(-r*t)*stats.norm.cdf(d2)
        )
    else:
        value = (
            k * np.exp(-r * t) * stats.norm.cdf(-d2) -
            s*np.exp(-q*t) * stats.norm.cdf(-d1)
        )
    return value
##############

# 二分法迭代计算隐波
def calculate_single_option_iv_by_bsm(
    cp, s, k, c, t, r, q,
    initial_iv=0.5, # 迭代起始值，如果上一个分钟有计算过隐波，这里把上一分钟的结果输入进来，有助于加快收敛
):

    c_est = 0  # 期权价格估计值
    top = 1  # 波动率上限
    floor = 0  # 波动率下限
    sigma = initial_iv  # 波动率初始值
    count = 0  # 计数器
    best_result = 0
    error = abs(c - c_est)
    last_error = error
    while error > 0.0001:
        c_est = bsm_value(cp, s, k, t, r, sigma, q)
        error = abs(c - c_est)
        if error < last_error:
            best_result = sigma

        # 根据价格判断波动率是被低估还是高估，并对波动率做修正
        count += 1
        if count > 100:  # 时间价值为0的期权是算不出隐含波动率的，因此迭代到一定次数就不再迭代了
            sigma = 0
            break

        if c - c_est > 0:  # f(x)>0
            floor = sigma
            sigma = (sigma + top)/2
        else:
            top = sigma
            sigma = (sigma + floor)/2
    return best_result

# 计算隐含分红率
# 我们目前不计算这个
def calculate_dividend_rate(
    underlying_price,    # 当前标的价格
    call_price,
    put_price,
    rest_days,           # 剩余时间
    exercise_price,        # 行权价
    free_rate,
    ):
    c = call_price
    c_p = put_price
    r = free_rate
    t = rest_days / 360
    k = exercise_price
    s = underlying_price
    q = -np.log((c+k*np.exp(-r*t)-c_p)/(s))/t
    return q

# 计算隐波和Greeks
def calculate_single_option_greeks(
    underlying_price,    # 当前标的价格
    option_price,        # 期权价格
    call_put,            # 期权方向, CALL=1 PUT=-1
    rest_days,           # 剩余时间，按自然日计算，也可以用小数来表示不完整的日子
    exercise_price,      # 行权价
    free_rate = 0.03,    # 无风险利率，如果没有数据，指定为3%
    dividend_rate = 0,   # 分红率，目前指定为0
    initial_iv = 0.5,    # 初始迭代的隐波
):
    cp = call_put
    s = underlying_price
    r = free_rate
    k = exercise_price
    t = rest_days / 360
    c = option_price
    q = dividend_rate
    sigma = calculate_single_option_iv_by_bsm(cp, s, k, c, t, r, q, initial_iv)
    delta, gamma, vega, theta = get_option_greeks(cp, s, k, t, r, sigma, q)
    # sigma就是iv
    return sigma, delta, gamma, vega, theta
