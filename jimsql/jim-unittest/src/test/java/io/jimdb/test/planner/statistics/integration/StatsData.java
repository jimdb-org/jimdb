/*
 * Copyright 2019 The JIMDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.jimdb.test.planner.statistics.integration;

public class StatsData {
  public static final String testAnalyzeRow0 = "id=5; isIndex=\u0000; rowCount=10; hId=5; hNdv=7; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=100, lower=40, totalCount=10, repeatedCount=2}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 86, 1}{0, 489, 1}{0, 781, 1}{0, 930, 2}{0, 967, 2}{0, 1419, 1}{0, 1947, 2}#{1, 1000, 1}{1, 1181, 2}{1, 1208, 1}{1, 1338, 1}{1, 1578, 2}{1, 1794, 2}{1, 1806, 1}#{2, 139, 1}{2, 573, 2}{2, 997, 1}{2, 1209, 2}{2, 1219, 1}{2, 1432, 2}{2, 1478, 1}#{3, 786, 1}{3, 840, 2}{3, 988, 1}{3, 1150, 1}{3, 1400, 2}{3, 1438, 1}{3, 1683, 2}#{4, 179, 2}{4, 471, 2}{4, 575, 1}{4, 822, 1}{4, 1657, 1}{4, 1837, 1}{4, 1934, 2}#; cDefaultValue=1; cNdv=7; cTotalCount=10";
  public static final String testAnalyzeRow1 = "id=0; isIndex=\u0001; rowCount=10; hId=0; hNdv=8; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=ffffffff, lower=\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001, totalCount=10, repeatedCount=1}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 212, 1}{0, 298, 2}{0, 547, 1}{0, 1069, 2}{0, 1358, 1}{0, 1509, 1}{0, 1602, 1}{0, 1934, 1}#{1, 289, 2}{1, 555, 1}{1, 923, 2}{1, 1138, 1}{1, 1469, 1}{1, 1517, 1}{1, 1660, 1}{1, 1752, 1}#{2, 343, 1}{2, 767, 1}{2, 1060, 1}{2, 1100, 1}{2, 1548, 2}{2, 1557, 2}{2, 1800, 1}{2, 1902, 1}#{3, 4, 1}{3, 125, 2}{3, 396, 1}{3, 460, 1}{3, 683, 1}{3, 777, 2}{3, 997, 1}{3, 1265, 1}#{4, 25, 1}{4, 139, 1}{4, 154, 1}{4, 194, 1}{4, 266, 1}{4, 750, 2}{4, 1908, 1}{4, 2045, 2}#; cDefaultValue=1; cNdv=8; cTotalCount=10";
  public static final String testAnalyzeRow2 = "id=950; isIndex=; rowCount=10; hId=950; hNdv=6; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001, lower=\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001, totalCount=10, repeatedCount=1}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 86, 1}{0, 489, 1}{0, 781, 2}{0, 1170, 1}{0, 1335, 2}{0, 1349, 3}#{1, 546, 3}{1, 564, 2}{1, 1000, 2}{1, 1338, 1}{1, 1622, 1}{1, 1806, 1}#{2, 26, 1}{2, 139, 1}{2, 1219, 2}{2, 1478, 1}{2, 1791, 3}{2, 1841, 2}#{3, 478, 1}{3, 988, 4}{3, 1070, 2}{3, 1150, 1}{3, 1438, 2}#{4, 185, 3}{4, 299, 2}{4, 822, 1}{4, 930, 1}{4, 1657, 2}{4, 1837, 1}#; cDefaultValue=1; cNdv=6; cTotalCount=10";

  public static final String testAnalyzeRow3 = "id=951; isIndex=\u0001; rowCount=10; hId=951; hNdv=10; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=13010019999, lower=13010010000, totalCount=10, repeatedCount=1}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 21, 1}{0, 142, 1}{0, 231, 1}{0, 419, 1}{0, 436, 1}{0, 522, 1}{0, 638, 1}{0, 1332, 1}{0, 1504, 1}{0, 1894, 1}#{1, 71, 1}{1, 157, 1}{1, 648, 1}{1, 1172, 1}{1, 1437, 1}{1, 1542, 1}{1, 1685, 1}{1, 1996, 1}{1, 2017, 1}{1, 2029, 1}#{2, 654, 1}{2, 658, 1}{2, 858, 1}{2, 894, 1}{2, 980, 1}{2, 1301, 1}{2, 1525, 1}{2, 1574, 1}{2, 1822, 1}{2, 1959, 1}#{3, 246, 1}{3, 424, 1}{3, 523, 1}{3, 668, 1}{3, 917, 1}{3, 1054, 1}{3, 1119, 1}{3, 1339, 1}{3, 1559, 1}{3, 1799, 1}#{4, 66, 1}{4, 212, 1}{4, 533, 1}{4, 583, 1}{4, 664, 1}{4, 678, 1}{4, 1074, 1}{4, 1639, 1}{4, 1646, 1}{4, 2024, 1}#; cDefaultValue=1; cNdv=10; cTotalCount=10";
  public static final String testAnalyzeRow4 = "id=77; isIndex=\u0001; rowCount=10; hId=77; hNdv=10; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=3133303130303139393939, lower=3133303130303130303030, totalCount=10, repeatedCount=1}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 21, 1}{0, 142, 1}{0, 231, 1}{0, 419, 1}{0, 436, 1}{0, 522, 1}{0, 638, 1}{0, 1332, 1}{0, 1504, 1}{0, 1894, 1}#{1, 71, 1}{1, 157, 1}{1, 648, 1}{1, 1172, 1}{1, 1437, 1}{1, 1542, 1}{1, 1685, 1}{1, 1996, 1}{1, 2017, 1}{1, 2029, 1}#{2, 654, 1}{2, 658, 1}{2, 858, 1}{2, 894, 1}{2, 980, 1}{2, 1301, 1}{2, 1525, 1}{2, 1574, 1}{2, 1822, 1}{2, 1959, 1}#{3, 246, 1}{3, 424, 1}{3, 523, 1}{3, 668, 1}{3, 917, 1}{3, 1054, 1}{3, 1119, 1}{3, 1339, 1}{3, 1559, 1}{3, 1799, 1}#{4, 66, 1}{4, 212, 1}{4, 533, 1}{4, 583, 1}{4, 664, 1}{4, 678, 1}{4, 1074, 1}{4, 1639, 1}{4, 1646, 1}{4, 2024, 1}#; cDefaultValue=1; cNdv=10; cTotalCount=10";
  public static final String testAnalyzeRow5 = "id=953; isIndex=\u0001; rowCount=10; hId=953; hNdv=7; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=\u0000\u0000\u0000\u0000\u0000\u0000\u0000d, lower=\u0000\u0000\u0000\u0000\u0000\u0000\u0000(, totalCount=10, repeatedCount=2}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 86, 1}{0, 489, 1}{0, 781, 1}{0, 930, 2}{0, 967, 2}{0, 1419, 1}{0, 1947, 2}#{1, 1000, 1}{1, 1181, 2}{1, 1208, 1}{1, 1338, 1}{1, 1578, 2}{1, 1794, 2}{1, 1806, 1}#{2, 139, 1}{2, 573, 2}{2, 997, 1}{2, 1209, 2}{2, 1219, 1}{2, 1432, 2}{2, 1478, 1}#{3, 786, 1}{3, 840, 2}{3, 988, 1}{3, 1150, 1}{3, 1400, 2}{3, 1438, 1}{3, 1683, 2}#{4, 179, 2}{4, 471, 2}{4, 575, 1}{4, 822, 1}{4, 1657, 1}{4, 1837, 1}{4, 1934, 2}#; cDefaultValue=1; cNdv=7; cTotalCount=10";
  public static final String testAnalyzeRow6 = "id=954; isIndex=\u0001; rowCount=10; hId=954; hNdv=10; hNullCount=0; hTotalCount=10.0; hBuckets=|Bucket{upper=Tom, lower=Bob, totalCount=10, repeatedCount=1}|; cRowCount=5; cColumnCount=2048; cHashTables={0, 78, 1}{0, 534, 1}{0, 620, 1}{0, 1315, 1}{0, 1325, 1}{0, 1530, 1}{0, 1541, 1}{0, 1780, 1}{0, 1910, 1}{0, 1958, 1}#{1, 174, 1}{1, 211, 1}{1, 690, 1}{1, 741, 1}{1, 878, 1}{1, 952, 1}{1, 1199, 1}{1, 1480, 1}{1, 1506, 1}{1, 1677, 1}#{2, 55, 1}{2, 270, 1}{2, 868, 1}{2, 929, 1}{2, 1136, 1}{2, 1232, 1}{2, 1370, 1}{2, 1396, 1}{2, 1620, 1}{2, 1645, 1}#{3, 366, 1}{3, 451, 1}{3, 537, 1}{3, 958, 1}{3, 1115, 1}{3, 1394, 1}{3, 1468, 1}{3, 1647, 1}{3, 1788, 1}{3, 1810, 1}#{4, 158, 1}{4, 206, 1}{4, 317, 1}{4, 462, 1}{4, 684, 1}{4, 833, 1}{4, 834, 1}{4, 1330, 1}{4, 1652, 1}{4, 1975, 1}#; cDefaultValue=1; cNdv=10; cTotalCount=10";
}