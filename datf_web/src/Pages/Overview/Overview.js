import { Card, CardContent, Grid } from '@mui/material';
import './overview.css';
import { useState } from 'react';

export default function Overview() {
    const [age, setAge] = useState('');

    const handleChange = (event) => {
        setAge(event.target.value);
    };
    return (
        <div className='main-container-layout'>
            <div className='container-layout'>
                <div className="heading">Overview</div>
                <>
                <select name="cars" id="cars" style={{ backgroundColor:'#E8E8E8' }} onChange={handleChange}>
                <option value="volvo">Last Year</option>
                <option value="volvo">2020</option>
                <option value="saab">2021</option>
                <option value="mercedes">2022</option>
                <option value="audi">2023</option>
                <option value="audi">2024</option>
            </select></>
            </div>
            <div className='sub-heading'>Execution Summary</div>
            <Grid container>
                <Grid item xs={12}>
                    <Card>
                        <CardContent>

                        </CardContent>
                    </Card>
                </Grid>
            </Grid>

        </div>
    )
}