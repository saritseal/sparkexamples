TODO : 
1. calculate a running mean, median over windows
1. To calculate a running median we can just concentrate on the rows in a window or we can use a heal to calculate the running median
1. calculate KS Test over the sample and find the pvalue over the last two data window samples
1. Calculate the Zscore over the windows
1. Calculate Skewness
1. Calculate Histogram for the data. In order to calculate the histogram you need the number of bins to calculate the number of bins use the following formulas
    a.  $bins = \sqrt{n}$ ; where n = row count
    b.  $bins = 2 * \sqrt[3]{n}$ : **Rice Rule**
    c.  $bin width = (2 * IQR) \div{\sqrt[3]{n}}$
            bins = $(max(data) - min(data)) \div {((2 * IQR) \div{\sqrt[3]{n}})} $
            $ IQR = Q3 - Q1 $
            $ Q1 = percentile(data, 25) $
            $ Q3 = percentile(data, 75) $
            $ Lower bound = Q1 - 1.5 * IQR $
            $ Upper bound = Q3 + 1.5 * IQR $
    