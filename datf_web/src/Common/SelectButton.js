import "./common.css";

export default function SelectButton(props) {
    const { label, bgcolor } = props;

    return (
        <>
            {/* <label for="cars">{label}</label> */}
            <select name="cars" id="cars" style={{ backgroundColor: { bgcolor } }}>
                <option value="volvo">Last Year</option>
                <option value="volvo">2020</option>
                <option value="saab">2021</option>
                <option value="mercedes">2022</option>
                <option value="audi">2023</option>
                <option value="audi">2024</option>
            </select></>
    )
}