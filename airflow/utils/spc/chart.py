# -*- coding: utf-8 -*-
import numpy as np
from airflow.utils.spc.table import A2, A3, D3, D4, B3, B4
from typing import List, Optional, Dict
import logging

_logger = logging.getLogger(__name__)


def covert2dArray(data: List[float], size: int) -> Optional[np.ndarray]:
    b = len(data) % size
    if b != 0:
        _logger.error(u"数据长度不正确!!! 自动截取")
    l = int(len(data) / size)
    offset = len(data) - b
    ret = np.array(data[:offset]).reshape(l, size)
    return ret


def xbar_sbar(data: np.ndarray, size: int, newdata=None) -> Optional[Dict]:
    assert size >= 2
    assert size <= 10
    newvalues = None

    assert data.ndim == 2

    X, S = [], []
    for xs in data:
        assert len(xs) == size
        S.append(np.std(xs, ddof=1))
        X.append(np.mean(xs))

    if newdata:
        newvalues = [np.mean(xs) for xs in newdata]

    sbar = np.mean(S)
    xbar = np.mean(X)

    lclx = xbar - A3[size] * sbar
    uclx = xbar + A3[size] * sbar

    ret = {
        "data": X,
        "center": xbar,
        "lower": lclx,
        "upper": uclx,
    }

    return ret


def xbar_rbar(data: np.ndarray, size: int, newdata=None) -> Optional[Dict]:
    """

    :rtype: 元组，数据，中线，下线，上线
    """
    assert size >= 2
    assert size <= 10
    assert data.ndim == 2

    newvalues = None

    R, X = [], []  # values
    for xs in data:
        assert len(xs) == size
        R.append(max(xs) - min(xs))
        X.append(np.mean(xs))

    if newdata:
        newvalues = [np.mean(xs) for xs in newdata]

    Rbar = np.mean(R)  # center
    Xbar = np.mean(X)

    lcl = Xbar - A2[size] * Rbar
    ucl = Xbar + A2[size] * Rbar

    ret = {
        "data": X,
        "center": Xbar,
        "lower": lcl,
        "upper": ucl,
    }

    return ret


def rbar(data: np.ndarray, size: int, newdata=None)->Optional[Dict]:
    """

    :rtype: 元组，数据，中线，下线，上线
    """
    assert size >= 2
    assert size <= 10
    assert data.ndim == 2

    newvalues = None

    R = []  # values
    for xs in data:
        assert len(xs) == size
        R.append(max(xs) - min(xs))

    if newdata:
        newvalues = [max(xs) - min(xs) for xs in newdata]

    Rbar = np.mean(R)  # center

    lcl = D3[size] * Rbar
    ucl = D4[size] * Rbar

    ret = {
        "data": R,
        "center": Rbar,
        "lower": lcl,
        "upper": ucl,
    }

    return ret


def u(data: np.ndarray, size: int, newdata=None) ->Optional[Dict]:
    """
    SPC U-charts
    :param data:
    :param size:
    :param newdata:
    :return:
    """
    sizes, data = data.T
    if size == 1:
        sizes, data = data, sizes

    data2 = sizes / data
    ubar = np.sum(sizes) / np.sum(data)

    lcl, ucl = [], []
    for i in data:
        lcl.append(ubar - 3 * np.sqrt(ubar / i))
        ucl.append(ubar + 3 * np.sqrt(ubar / i))

    ret = {
        "data": data2,
        "center": ubar,
        "lower": lcl,
        "upper": ucl,
    }

    return ret


def np_chart(data: np.ndarray, size: int, newdata=None)->Optional[Dict]:
    sizes, data = data.T
    if size == 1:
        sizes, data = data, sizes

    # the samples must have the same size for this charts
    assert np.mean(sizes) == sizes[0]

    p = np.mean([float(d) / sizes[0] for d in data])
    pbar = np.mean(data)

    lcl = pbar - 3 * np.sqrt(pbar * (1 - p))
    ucl = pbar + 3 * np.sqrt(pbar * (1 - p))

    ret = {
        "data": data,
        "center": pbar,
        "lower": lcl,
        "upper": ucl,
    }

    return ret


def sbar(data: np.ndarray, size: int, newdata=None)->Optional[Dict]:
    """

    :rtype: 元组，数据，中线，下线，上线
    """
    assert size >= 2
    assert size <= 10
    assert data.ndim == 2
    newvalues = None

    S = []
    for xs in data:
        assert len(xs) == size
        S.append(np.std(xs, ddof=1))

    if newdata:
        newvalues = [np.std(xs, ddof=1) for xs in newdata]

    sbar = np.mean(S)

    lcls = B3[size] * sbar
    ucls = B4[size] * sbar

    ret = {
        "data": S,
        "center": sbar,
        "lower": lcls,
        "upper": ucls,
    }

    return ret


def cpk(data: List[float], usl: float, lsl: float) -> Optional[float]:
    if not usl or not lsl:
        return None
    if not data:
        return None
    sigma = np.std(data)
    m = np.mean(data)
    if usl < m or m < lsl:
        return None
    Cpu = float(usl - m) / (3 * sigma)
    Cpl = float(m - lsl) / (3 * sigma)
    Cpk = np.min([Cpu, Cpl])
    return Cpk


def cmk(data: List, usl: float, lsl: float) -> Optional[float]:
    if not usl or not lsl:
        return None
    if not data:
        return None
    avg = np.average(data)
    sigma = np.std(data)
    if usl < avg or lsl > avg:
        return None
    a = (usl - avg) / (3 * sigma)
    b = (avg - lsl) / (3 * sigma)
    cmk = np.min([a, b])
    return cmk
