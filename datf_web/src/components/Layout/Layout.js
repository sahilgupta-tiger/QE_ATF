import React from 'react';
import { Outlet } from 'react-router-dom'
import SideBar from '../SideBar/SideBar';
import "./layout.css";

export default function Layout() {

    return (
        <div className='layout' >
            <SideBar />
            <div style={{marginTop:50, overflow:'hidden',backgroundColor:'#F4F4F4'}}>
                <Outlet />
            </div>
        </div>
    )

}