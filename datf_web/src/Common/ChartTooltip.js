import _ from 'lodash';

export default function ChartTooltip({ active, payload, label }) {
    if (active && payload && payload.length) {
        return (
            <div className="tooltip-div">
                {label && <p className="label">{label}</p>}
                {payload.map((row, i) => (
                    <p key={i} className="item" style={{ color: row.color }}>
                        {row.name}: {getValue(row.value)} {row.unit || ""}
                    </p>
                ))}
            </div>
        );
    }

    return null;
}

function getValue(v) {
    if (_.isNumber(v)) {
        return new Intl.NumberFormat('en-IN').format(v)
    }

    return v
}