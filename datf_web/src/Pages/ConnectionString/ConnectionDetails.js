import { Card, CardContent, Divider, Grid } from "@mui/material";
import { useNavigate, useParams } from "react-router-dom"
import { connectData } from "../Extraction/Data";
import { useForm } from "react-hook-form";
import FormProvider from "../../Common/FormProvider";
import RHFTextField from "../../Common/RHFTextField";
import { useEffect } from "react";

export default function ConnectionDetails() {
    const { id } = useParams();
    const navigate = useNavigate();
    const methods = useForm();
    const { handleSubmit, reset, setValue } = methods;

    const onSubmit = (data) => {
        console.log(data)
    }


    useEffect(()=>{
        setValue(`connection_type`, connectData[0].connnectiontype);
        setValue(`connection_name`, connectData[0].connectname)
        setValue(`host`, connectData[0].host)
        setValue(`port`, connectData[0].port)
        setValue(`database`, connectData[0].database)
        setValue(`user`, connectData[0].user)
        setValue(`password`, connectData[0].password)

    },[])
    return (
        <div className='main-container-layout'>
            <div className="heading image" onClick={() => navigate('/connection_history')}><img src='/img/asset_19.svg' alt='back' className='image' /> Connection String Details</div>
            <Card>
                <CardContent>
                    <div className='card-container ht'>
                        <div className="sub-heading">{id}</div>
                    </div>
                    <Divider />
                    <Grid container spacing={2}>
                        <Grid item xs={6}>
                            <div className='first-container'>
                                <FormProvider methods={methods} onSubmit={handleSubmit(onSubmit)}>
                                    <Grid container spacing={3} alignItems="center">
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>Connection Type</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='connection_type' className='inputbox'  />
                                            {/* <input type='text' className='inputbox' value={connectData[0].connnectiontype}/> */}
                                        </Grid>
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>Connection Name</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='connection_name' className='inputbox' />
                                            {/* <input type='text' className='inputbox' value={connectData[0].connectname}/> */}
                                        </Grid>
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>Host</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='host' className='inputbox' />
                                            {/* <input type='text' className='inputbox' value={connectData[0].host}/> */}
                                        </Grid>
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>Port</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='port' className='inputbox' />
                                            {/* <input type='text' className='inputbox' value={connectData[0].port}/> */}
                                        </Grid>
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>Database</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='database' className='inputbox'  />
                                            {/* <input type='text' className='inputbox' value={connectData[0].database}/> */}
                                        </Grid>
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>User</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='user' className='inputbox' />
                                            {/* <input type='text' className='inputbox' value={connectData[0].user}/> */}
                                        </Grid>
                                        <Grid item xs={3} >
                                            <div className='first-container-headings'>Password</div>
                                        </Grid>
                                        <Grid item xs={9} >
                                            <RHFTextField name='password' className='inputbox'  />
                                            {/* <input type='text' className='inputbox' value={connectData[0].password}/> */}
                                        </Grid>
                                        <Grid item xs={12} >
                                            <button className='add-btn' type="submit"> <img src='/img/Group_1389.svg' alt='add' className="add-image" /> Add More Field</button>
                                        </Grid>
                                    </Grid>
                                </FormProvider>
                            </div>
                        </Grid>
                        <Grid item xs={6}>
                            <div className='second-container'>
                                <div className="second-conatiner-object">
                                    <pre>{JSON.stringify(connectData[0], null, 2)}</pre>
                                </div>
                            </div>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>
        </div>
    )
}