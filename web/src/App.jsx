import { lazy, Suspense } from "react";
import { BrowserRouter, Routes, Route, Link } from "react-router-dom";

// Lazy-loaded routes — each chunk is downloaded only when the user navigates
// to that route. WalletGraph (with D3) is not bundled into the initial JS.
const Top100Table = lazy(() => import("./components/Top100Table"));
const WalletGraph = lazy(() => import("./components/WalletGraph"));

function PageLoader() {
  return (
    <div className="page-loader">
      <div className="spinner" />
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <header>
        <h1>ETH Transaction Analytics</h1>
        <nav>
          <Link to="/">Top 100</Link>
        </nav>
      </header>
      <div className="container">
        <Suspense fallback={<PageLoader />}>
          <Routes>
            <Route path="/" element={<Top100Table />} />
            <Route path="/wallet/:address" element={<WalletGraph />} />
          </Routes>
        </Suspense>
      </div>
    </BrowserRouter>
  );
}
