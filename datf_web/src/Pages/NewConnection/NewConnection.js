import { useNavigate } from "react-router-dom";
import '../Overview/overview.css';
import "../Extraction/execution.css";
import "../TestCaseSetUp/testcase.css";
import "./newconnection.css";
import { Card, CardContent, Divider, Grid, } from '@mui/material';
import FormProvider from "../../Common/FormProvider";
import { useForm } from "react-hook-form";
import RHFTextField from "../../Common/RHFTextField";

export default function NewConnection() {
    const navigate = useNavigate();
    const methods = useForm();
    const { handleSubmit, reset } = methods;

    const onSubmit = (data) => {
        console.log(data)
    }
    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/connection_history')}><img src='/img/asset_19.svg' alt='back' className='image' /> New Connection String</div>
            <Grid container>
                <Grid item xs={12} >
                    <Card>
                        <CardContent >
                            <div className='card-container'>
                                <div className="sub-heading">Connection Details</div>
                            </div>
                            <Divider />

                            <Grid container spacing={2}>
                                <Grid item xs={6}>
                                    <div className='first-container'>
                                        <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                            <Grid container spacing={3} alignItems="center">
                                                <Grid item xs={3} >
                                                    Connection Type
                                                </Grid>
                                                <Grid item xs={9} >
                                                    {/* <input type='text' className='inputbox'/> */}
                                                    <RHFTextField name='connection_type' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={3} >
                                                    Connection Name
                                                </Grid>
                                                <Grid item xs={9} >
                                                    <RHFTextField name='connection_name' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={3} >
                                                    Host
                                                </Grid>
                                                <Grid item xs={9} >
                                                    <RHFTextField name='host' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={3} >
                                                    Port
                                                </Grid>
                                                <Grid item xs={9} >
                                                    <RHFTextField name='port' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={3} >
                                                    Database
                                                </Grid>
                                                <Grid item xs={9} >
                                                    <RHFTextField name='database' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={3} >
                                                    User
                                                </Grid>
                                                <Grid item xs={9} >
                                                    <RHFTextField name='user' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={3} >
                                                    Password
                                                </Grid>
                                                <Grid item xs={9} >
                                                    <RHFTextField name='password' className='inputbox' />
                                                </Grid>
                                                <Grid item xs={12} >
                                                    <button className='add-btn' type='submit'> <img src='/img/Group_1389.svg' alt='add' className="add-image" /> Add More Field</button>
                                                </Grid>
                                            </Grid>
                                        </FormProvider>
                                    </div>

                                </Grid>
                                <Grid item xs={6}>
                                    <div className='second-container'>
                                        <div className='second-container-text'>Fill the Connection details</div>
                                    </div>
                                </Grid>
                            </Grid>


                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div>
    )
}