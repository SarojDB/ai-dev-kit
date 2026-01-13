import { Link, useLocation } from 'react-router-dom';
import { useUser } from '@/contexts/UserContext';

interface TopBarProps {
  projectName?: string;
}

export function TopBar({ projectName }: TopBarProps) {
  const location = useLocation();
  const { user } = useUser();

  // Extract username from email for display
  const displayName = user?.split('@')[0] || '';

  return (
    <header className="fixed top-0 left-0 right-0 z-30 h-[var(--header-height)] bg-[var(--color-background)]/70 backdrop-blur-xl backdrop-saturate-150 border-b border-[var(--color-border)]/40 shadow-sm">
      <div className="flex items-center justify-between h-full px-4 lg:px-6">
        {/* Left Section - Logo & Name */}
        <div className="flex items-center gap-4">
          {/* Logo */}
          <Link to="/" className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-[var(--color-accent-primary)] flex items-center justify-center">
              <svg
                className="w-5 h-5 text-white"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"
                />
              </svg>
            </div>
            <h1 className="text-xl font-semibold tracking-tight text-[var(--color-text-heading)]">
              Databricks AI Dev Kit
            </h1>
          </Link>

          {/* Project Name Breadcrumb */}
          {projectName && (
            <>
              <span className="text-[var(--color-text-muted)]">/</span>
              <span className="text-[var(--color-text-primary)] font-medium truncate max-w-[200px]">
                {projectName}
              </span>
            </>
          )}
        </div>

        {/* Right Section - Navigation & User */}
        <div className="flex items-center gap-6">
          {/* Navigation */}
          <nav className="flex items-center gap-1">
            <Link
              to="/"
              className={`
                relative px-4 py-2 text-sm font-medium transition-colors duration-300
                ${
                  location.pathname === '/'
                    ? 'text-[var(--color-foreground)]'
                    : 'text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]'
                }
              `}
            >
              <span className="relative z-10">Projects</span>
              {location.pathname === '/' && (
                <span className="absolute bottom-1.5 left-4 right-4 h-0.5 bg-[var(--color-accent-primary)] rounded-full" />
              )}
            </Link>
            <Link
              to="/doc"
              className={`
                relative px-4 py-2 text-sm font-medium transition-colors duration-300
                ${
                  location.pathname === '/doc'
                    ? 'text-[var(--color-foreground)]'
                    : 'text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]'
                }
              `}
            >
              <span className="relative z-10">Docs</span>
              {location.pathname === '/doc' && (
                <span className="absolute bottom-1.5 left-4 right-4 h-0.5 bg-[var(--color-accent-primary)] rounded-full" />
              )}
            </Link>
          </nav>

          {/* User Email */}
          {displayName && (
            <div
              className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-[var(--color-bg-secondary)] border border-[var(--color-border)] shadow-sm"
              title={user || undefined}
            >
              <div className="w-6 h-6 rounded-full bg-[var(--color-accent-primary)] flex items-center justify-center text-white text-xs font-medium">
                {displayName.charAt(0).toUpperCase()}
              </div>
              <span className="text-sm text-[var(--color-text-primary)] max-w-[120px] truncate">
                {displayName}
              </span>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
