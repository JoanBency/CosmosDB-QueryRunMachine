function calculateMinMax(currentValue, minValue, maxValue) {
    if (currentValue < minValue) { newMinValue = currentValue; }
    else { newMinValue = minValue; }
    if (currentValue > maxValue) { newMaxValue = currentValue; }
    else { newMaxValue = maxValue; }
    return { newMinValue, newMaxValue }
}

module.exports = { calculateMinMax };