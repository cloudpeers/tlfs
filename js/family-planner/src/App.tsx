import React from 'react'
import logo from './logo.svg'
import './App.css'
import { Route, Router } from 'react-router'
import { Routes } from 'react-router-dom'

const App = async () => {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.tsx</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
      </header>
    </div>
  )
}

const AppRoutes = () => (<Routes>
  <Route path = "shopping"><div>Shopping List</div></Route>
</Routes>)

export default App
