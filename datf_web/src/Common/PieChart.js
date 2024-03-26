import React, { useState } from 'react';
import { PieChart as PieChartRe, Pie, Cell, ResponsiveContainer, Tooltip, Legend, Sector } from 'recharts';
import { formatNumber } from './utilty/UtilityHelper';
import ChartTooltip from './ChartTooltip';
import getColor from './getColor';
// import _ from 'lodash';

export function HalfPieChart({ width = '100%', height, containerHeight, cy, data }) {
    const [activeIndex, setActiveIndex] = useState(0)

    return (
        <ResponsiveContainer width={width} height={containerHeight || 140}>
            <PieChartRe width={width} height={height || 200}>
                <Pie
                    data={data}
                    startAngle={180}
                    endAngle={0}
                    dataKey="value"
                    innerRadius={60}
                    outerRadius={90}
                    activeIndex={activeIndex}
                    onMouseEnter={(_, index) => setActiveIndex(index)}
                    activeShape={renderActiveShapeHalfPie}
                    cy={cy || 90}
                    paddingAngle={1}
                    animationDuration={1500}
                    animationEasing="ease-in-out"
                >
                    {data.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.fill || getColor(index)} />
                    ))}
                </Pie>
                {/* <Tooltip content={<ChartTooltip />} /> */}
                {/* <Legend iconSize="10px" iconType="circle" wrapperStyle={{ fontSize: 11, fontWeight: 500 }} /> */}
            </PieChartRe>
        </ResponsiveContainer>
    );
}

export function PieChart({ height, width, data, innerRadius = 60, outerRadius = 90, tooltip = true, legend = true }) {
    const [activeIndex, setActiveIndex] = useState(0)

    return (
        <ResponsiveContainer width={width || "100%"} height={height || 200}>
            <PieChartRe width={width || "100%"} height={height || 200}>
                <Pie
                    data={data}
                    dataKey="value"
                    activeIndex={activeIndex}
                    onMouseEnter={(_, index) => setActiveIndex(index)}
                    innerRadius={innerRadius}
                    outerRadius={outerRadius}
                    activeShape={renderActiveShape}
                    paddingAngle={1}
                    animationDuration={1500}
                    animationEasing="ease-in-out"
                >
                    {data.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.fill || getColor(index)} />
                    ))}
                </Pie>

                {tooltip && <Tooltip content={<ChartTooltip />} />}
                {legend && <Legend iconSize="10px" iconType="circle" wrapperStyle={{ fontSize: 11, fontWeight: 500 }} />}
            </PieChartRe>
        </ResponsiveContainer>
    )
}

const renderActiveShapeHalfPie = (props) => {
    const { cx, cy, innerRadius, outerRadius, startAngle, endAngle, fill, percent, value, name } = props;

    return (
        <g>
            <text x={cx} y={cy - 10} dy={8} textAnchor="middle" fontSize="12px" fontWeight={500} fill="#000">
                TOTAL:{formatNumber((value / (percent * 100)) * 100)}
            </text>
            <text x={cx} y={cy + 15} dy={8} textAnchor="middle" fontSize="11px" fontWeight={500} fill={fill}>
                {name.toUpperCase()}: {`${formatNumber(value)} (${(percent * 100).toFixed(2)}%)`}
            </text>
            <Sector
                cx={cx}
                cy={cy}
                innerRadius={innerRadius}
                outerRadius={outerRadius}
                startAngle={startAngle}
                endAngle={endAngle}
                fill={fill}
            />
            <Sector
                cx={cx}
                cy={cy}
                startAngle={startAngle}
                endAngle={endAngle}
                innerRadius={outerRadius + 1}
                outerRadius={outerRadius + 3}
                fill={fill}
            />
        </g>
    );
};

const renderActiveShape = (props) => {
    const RADIAN = Math.PI / 180;
    const { cx, cy, midAngle, innerRadius, outerRadius, startAngle, endAngle, fill, percent, value, name } = props;
    const sin = Math.sin(-RADIAN * midAngle);
    const cos = Math.cos(-RADIAN * midAngle);
    // const sx = cx + (outerRadius + 10) * cos;
    // const sy = cy + (outerRadius + 10) * sin;
    // const mx = cx + (outerRadius + 30) * cos;
    // const my = cy + (outerRadius + 30) * sin;
    // const ex = mx + (cos >= 0 ? 1 : -1) * 22;
    // const ey = my;
    // const textAnchor = cos >= 0 ? 'start' : 'end';

    return (
        <g>
            <text x={cx} y={cy-10} dy={8} textAnchor="middle" fontSize="11px" fontWeight={500} fill="#000">
                TOTAL:{formatNumber((value / (percent * 100)) * 100)}
            </text>
            <text x={cx} y={cy+5} dy={8} textAnchor="middle" fontSize="11px" fontWeight={500} fill={fill}>
            {`${name}`}: {`${formatNumber(value)} (${(percent * 100).toFixed(2)}%)`}
            </text>
            <Sector
                cx={cx}
                cy={cy}
                innerRadius={innerRadius}
                outerRadius={outerRadius}
                startAngle={startAngle}
                endAngle={endAngle}
                fill={fill}
            />
            <Sector
                cx={cx }
                cy={cy}
                startAngle={startAngle}
                endAngle={endAngle}
                innerRadius={outerRadius + 2}
                outerRadius={outerRadius + 5}
                fill={fill}
            />
        </g>
    );
};